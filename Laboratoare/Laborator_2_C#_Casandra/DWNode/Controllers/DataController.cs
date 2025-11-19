using Microsoft.AspNetCore.Mvc;
using Cassandra;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DWNode.Models; // Use the shared Item model

[ApiController]
[Route("api/data")]
public class DataController : ControllerBase
{
     private readonly Cassandra.ISession _cassandra;

     public DataController(CassandraService cassandraService)
     {
          _cassandra = cassandraService.Session;
     }

     // ====================
     // 1️ GET — Client reads data
     // GET /api/data/{id}
     // GET /api/data?offset=1&limit=4
     // ====================
     [HttpGet("{id}")]
     public async Task<ActionResult<Item>> GetItem(int id)
     {
          var ps = _cassandra.Prepare("SELECT id, name FROM items_by_id WHERE id = ?");
          var rs = await _cassandra.ExecuteAsync(ps.Bind(id));

          var row = rs.FirstOrDefault();
          if (row == null)
               return NotFound(new { message = $"Item {id} not found" });

          return Ok(new Item
          {
               Id = row.GetValue<int>("id"),
               Name = row.GetValue<string>("name")
          });
     }

     [HttpGet]
     public async Task<ActionResult<IEnumerable<Item>>> GetItems([FromQuery] int offset = 0, [FromQuery] int limit = 10)
     {
          // Cassandra does not support OFFSET, so we fetch all and skip in memory
          var rs = await _cassandra.ExecuteAsync(new SimpleStatement("SELECT id, name FROM items_by_id"));
          var list = rs.Select(row => new Item
          {
               Id = row.GetValue<int>("id"),
               Name = row.GetValue<string>("name")
          })
          .Skip(offset)
          .Take(limit)
          .ToList();

          return Ok(list);
     }

     // ====================
     // 2️ PUT — Storage nodes send data to DW
     // PUT /api/data/{id}
     // ====================
     [HttpPut("{id}")]
     public async Task<ActionResult> PutItem(int id, [FromBody] Item item)
     {
          var ps = _cassandra.Prepare("INSERT INTO items_by_id (id, name) VALUES (?, ?)");
          await _cassandra.ExecuteAsync(ps.Bind(id, item.Name));

          Console.WriteLine($"[PUT] Stored item {id}: {item.Name}");
          return Ok(new { status = "updated", id });
     }

     // ====================
     // 3️ POST — Client sends modifications to DW
     // POST /api/data/update
     // ====================
     [HttpPost("update")]
     public async Task<ActionResult> PostItemUpdate([FromBody] Item item)
     {
          var ps = _cassandra.Prepare("INSERT INTO items_by_id (id, name) VALUES (?, ?)");
          await _cassandra.ExecuteAsync(ps.Bind(item.Id, item.Name));

          Console.WriteLine($"[POST] Client updated item {item.Id}: {item.Name}");
          return Ok(new { status = "modification received", id = item.Id });
     }

     // ====================
     // 4️ PUSH — DW pushes update to source node
     // POST /api/data/push
     // ====================
     [HttpPost("push")]
     public async Task<ActionResult> PushUpdate([FromBody] Item item)
     {
          var ps = _cassandra.Prepare("INSERT INTO items_by_id (id, name) VALUES (?, ?)");
          await _cassandra.ExecuteAsync(ps.Bind(item.Id, item.Name));

          Console.WriteLine($"[PUSH] DW stored update: {item.Id} - {item.Name}");
          return Ok(new { pushed = true, id = item.Id });
     }
}

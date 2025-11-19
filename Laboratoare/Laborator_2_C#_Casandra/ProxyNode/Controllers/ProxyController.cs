using Microsoft.AspNetCore.Mvc;
using System.Net.Http;
using Newtonsoft.Json;
using ProxyNode.Models;

[ApiController]
[Route("proxy")]
public class ProxyController : ControllerBase
{
     private readonly HttpClient _http = new HttpClient();
     private readonly LBService _lb;
     private readonly RedisCache _cache;

     public ProxyController(LBService lb, RedisCache cache)
     {
          _lb = lb;
          _cache = cache;
     }

     // ====================
     // GET /proxy/data/{id}
     // ====================
     [HttpGet("data/{id}")]
     public async Task<IActionResult> GetProxy(int id)
     {
          string cacheKey = $"data:{id}";
          string acceptHeader = Request.Headers["Accept"].ToString();
          Console.WriteLine($"[Proxy][GET] Client requested format: {acceptHeader}");

          bool wantsXml = acceptHeader.Contains("application/xml", StringComparison.OrdinalIgnoreCase);

          var cached = await _cache.Get(cacheKey);
          if (cached != null)
          {
               Console.WriteLine($"[Proxy][GET] Cache hit for id={id}: {cached}");
               if (wantsXml)
               {
                    string xml = ConvertJsonToXml(cached);
                    return Content(xml, "application/xml");
               }
               return Content(cached, "application/json");
          }

          string server = _lb.NextServer();
          string url = $"{server}/api/data/{id}";

          var result = await _http.GetStringAsync(url);
          Console.WriteLine($"[Proxy][GET] Forwarded request to {server}, returned: {result}");

          await _cache.Set(cacheKey, result, 30);

          if (wantsXml)
          {
               string xml = ConvertJsonToXml(result);
               return Content(xml, "application/xml");
          }

          return Content(result, "application/json");
     }

     private string ConvertJsonToXml(string json)
     {
          var doc = Newtonsoft.Json.JsonConvert.DeserializeXmlNode(json, "Root");
          return doc?.OuterXml ?? "<Root></Root>";
     }

     // ====================
     // PUT /proxy/data/{id}
     // ====================
     [HttpPut("data/{id}")]
     public async Task<IActionResult> PutProxy(int id, [FromBody] Item item)
     {
          Console.WriteLine($"[Proxy][PUT] Forwarding PUT for id={id} with payload: {JsonConvert.SerializeObject(item)}");

          string server = _lb.NextServer();
          string url = $"{server}/api/data/{id}";

          var json = JsonConvert.SerializeObject(item);
          var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
          var response = await _http.PutAsync(url, content);

          var responseBody = await response.Content.ReadAsStringAsync();
          return Content(responseBody, "application/json");
     }

     // ====================
     // POST /proxy/data/update
     // ====================
     [HttpPost("data/update")]
     public async Task<IActionResult> PostProxy([FromBody] Item item)
     {
          Console.WriteLine($"[Proxy][POST] Forwarding POST update for id={item.Id} with payload: {JsonConvert.SerializeObject(item)}");

          string server = _lb.NextServer();
          string url = $"{server}/api/data/update";

          var json = JsonConvert.SerializeObject(item);
          var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
          var response = await _http.PostAsync(url, content);

          var responseBody = await response.Content.ReadAsStringAsync();
          return Content(responseBody, "application/json");
     }

     // ====================
     // POST /proxy/data/push
     // ====================
     [HttpPost("data/push")]
     public async Task<IActionResult> PushProxy([FromBody] Item item)
     {
          Console.WriteLine($"[Proxy][PUSH] Forwarding PUSH for id={item.Id} with payload: {JsonConvert.SerializeObject(item)}");

          string server = _lb.NextServer();
          string url = $"{server}/api/data/push";

          var json = JsonConvert.SerializeObject(item);
          var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
          var response = await _http.PostAsync(url, content);

          var responseBody = await response.Content.ReadAsStringAsync();
          return Content(responseBody, "application/json");
     }

}

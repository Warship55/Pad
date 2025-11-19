using StackExchange.Redis;
using System.Threading.Tasks;

public class RedisCache
{
     private readonly IDatabase _db;

     public RedisCache()
     {
          var redis = ConnectionMultiplexer.Connect("localhost:6379"); // your Redis server
          _db = redis.GetDatabase();
     }

     // Get a string from Redis
     public async Task<string?> Get(string key)
     {
          return await _db.StringGetAsync(key);
     }

     // Set a string in Redis with expiration in seconds
     public async Task Set(string key, string value, int expireSeconds)
     {
          await _db.StringSetAsync(key, value, TimeSpan.FromSeconds(expireSeconds));
     }
}

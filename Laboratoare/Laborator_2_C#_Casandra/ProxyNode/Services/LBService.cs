using Microsoft.AspNetCore.Mvc;
using static System.Net.WebRequestMethods;
using System.Net.Http;


public class LBService
{

     private int _counter = 0;
     private readonly string[] servers = {
        "http://localhost:5113", // DW1
        "http://localhost:5114"  // DW2
    };

     public string NextServer()
     {
          int index = Interlocked.Increment(ref _counter) % servers.Length;
          string server = servers[index];
          Console.WriteLine($"[LBService] Selected server: {server}");
          return server;
     }

}



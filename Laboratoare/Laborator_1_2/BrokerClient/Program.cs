using Grpc.Core;
using BrokerSystem;
using Grpc.Net.Client;

class Program
{
     static async Task Main(string[] args)
     {
          if (args.Length == 0)
          {
               Console.WriteLine("Usage:");
               Console.WriteLine(" send <tag1,tag2,...> <message>");
               Console.WriteLine(" receive <tag1,tag2,...>");
               return;
          }

          var mode = args[0].ToLower();
          using var channel = GrpcChannel.ForAddress("http://localhost:5000");
          var client = new BrokerService.BrokerServiceClient(channel);

          if (mode == "send" && args.Length >= 3)
          {
               var tags = args[1].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
               var content = string.Join(" ", args, 2, args.Length - 2);

               var response = await client.SendMessageAsync(new Message
               {
                    Sender = "Anonymous",
                    Tags = { tags },
                    Content = content
               });

               Console.WriteLine($"Server: {response.Info}");
          }
          else if (mode == "receive" && args.Length >= 2)
          {
               var tags = args[1].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

               using var streaming = client.Subscribe(new SubscribeRequest
               {
                    Tags = { tags }
               });

               Console.WriteLine($"Connected. Waiting for messages with tags: [{string.Join(", ", tags)}]");

               try
               {
                    await foreach (var msg in streaming.ResponseStream.ReadAllAsync())
                    {
                         if (msg.Tags.Any(t => tags.Contains(t)))
                              Console.WriteLine($"[{string.Join(", ", msg.Tags)}] {msg.Sender} -> {msg.Content}");
                    }
               }
               catch (Exception ex)
               {
                    Console.WriteLine($"Stream closed: {ex.Message}");
               }
          }
          else
          {
               Console.WriteLine("Invalid parameters.");
          }
     }
}

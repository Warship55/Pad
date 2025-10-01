using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using BrokerSystem;
using Grpc.Core;

namespace BrokerServer.Services
{
     public class BrokerServiceImpl : BrokerService.BrokerServiceBase
     {
          // Cozi per tag pentru stocarea mesajelor
          private static readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> Queues = new();

          // Clienți conectați și tag-urile lor
          private static readonly ConcurrentDictionary<IServerStreamWriter<Message>, string[]> ActiveReceivers = new();

          public override Task<Response> SendMessage(Message request, ServerCallContext context)
          {
               if (request.Tags.Count == 0)
                    return Task.FromResult(new Response { Success = false, Info = "Tags are required." });

               // Adăugăm mesajul în coada fiecărui tag
               foreach (var tag in request.Tags)
               {
                    var queue = Queues.GetOrAdd(tag, _ => new ConcurrentQueue<Message>());
                    queue.Enqueue(request);
               }

               Console.WriteLine($"[Server] Message from {request.Sender}: [{string.Join(", ", request.Tags)}] {request.Content}");

               // Livrare imediată către clienții conectați cu tag corespunzător
               foreach (var receiver in ActiveReceivers.ToArray())
               {
                    var matchingTags = request.Tags.Where(t => receiver.Value.Contains(t)).ToList();
                    if (matchingTags.Any())
                    {
                         _ = Task.Run(async () =>
                         {
                              try
                              {
                                   await receiver.Key.WriteAsync(new Message
                                   {
                                        Sender = request.Sender,
                                        Content = request.Content,
                                        Tags = { matchingTags }
                                   });
                                   Console.WriteLine($"[Server] Delivered: [{string.Join(", ", matchingTags)}] {request.Content}");
                              }
                              catch
                              {
                                   ActiveReceivers.TryRemove(receiver.Key, out _);
                              }
                         });
                    }
               }

               return Task.FromResult(new Response { Success = true, Info = "Message registered for matching clients." });
          }

          public override async Task Subscribe(SubscribeRequest request, IServerStreamWriter<Message> responseStream, ServerCallContext context)
          {
               if (request.Tags.Count == 0)
                    return;

               // Adăugăm clientul la lista de clienți activi cu tag-urile sale
               ActiveReceivers.TryAdd(responseStream, request.Tags.ToArray());

               Console.WriteLine($"[Server] Client subscribed with tags: [{string.Join(", ", request.Tags)}]");

               try
               {
                    // Livrăm mesajele deja existente pentru tag-urile abonatului
                    foreach (var tag in request.Tags)
                    {
                         if (Queues.TryGetValue(tag, out var queue))
                         {
                              var temp = queue.ToArray();
                              foreach (var msg in temp)
                              {
                                   var filteredTags = msg.Tags.Where(t => request.Tags.Contains(t)).ToList();
                                   if (filteredTags.Any())
                                   {
                                        await responseStream.WriteAsync(new Message
                                        {
                                             Sender = msg.Sender,
                                             Content = msg.Content,
                                             Tags = { filteredTags }
                                        });
                                        Console.WriteLine($"[Server] Delivered existing: [{string.Join(", ", filteredTags)}] {msg.Content}");
                                   }
                              }
                         }
                    }

                    // Livrare continuă pentru mesajele viitoare
                    while (!context.CancellationToken.IsCancellationRequested)
                    {
                         await Task.Delay(200);
                    }
               }
               finally
               {
                    ActiveReceivers.TryRemove(responseStream, out _);
                    Console.WriteLine("[Server] Client disconnected.");
               }
          }
     }
}

using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

class SubscriberInfo
{
    public Socket Socket { get; set; }
    public string[] Tags { get; set; }
}

class Broker
{
    static ConcurrentDictionary<Socket, SubscriberInfo> subscribers = new ConcurrentDictionary<Socket, SubscriberInfo>();
    static ConcurrentQueue<string> messageHistory = new ConcurrentQueue<string>(); // toate mesajele

    static void Main()
    {
        IPEndPoint endPoint = new IPEndPoint(IPAddress.Loopback, 5001);
        Socket listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(endPoint);
        listener.Listen(50);

        Console.WriteLine("[Broker] Started on port 5001...");

        while (true)
        {
            Socket client = listener.Accept();
            Thread t = new Thread(() => HandleClient(client));
            t.Start();
        }
    }

    static void HandleClient(Socket client)
    {
        try
        {
            byte[] buffer = new byte[2048];
            int received = client.Receive(buffer);
            string message = Encoding.UTF8.GetString(buffer, 0, received).Trim();

            if (message.StartsWith("SUBSCRIBE"))
            {
                string[] parts = message.Split(':');
                string[] tags = parts[1].Split(',');

                var sub = new SubscriberInfo { Socket = client, Tags = tags };
                subscribers[client] = sub;

                Console.WriteLine($"[Broker] New subscriber for tags: {string.Join(",", tags)}");

                // Trimite toate mesajele istorice relevante
                foreach (var msg in messageHistory)
                {
                    if (MessageMatches(sub, msg))
                    {
                        try { sub.Socket.Send(Encoding.UTF8.GetBytes(msg)); }
                        catch { }
                    }
                }

                // Thread care menține socketul deschis
                Thread listenThread = new Thread(() =>
                {
                    try
                    {
                        while (true)
                        {
                            int r = client.Receive(buffer);
                            if (r == 0) break;
                        }
                    }
                    catch { }
                    finally
                    {
                        subscribers.TryRemove(client, out _);
                        try { client.Close(); } catch { }
                        Console.WriteLine("[Broker] Subscriber disconnected.");
                    }
                });
                listenThread.IsBackground = true;
                listenThread.Start();
            }
            else if (message.StartsWith("UNSUBSCRIBE"))
            {
                subscribers.TryRemove(client, out _);
                client.Close();
                Console.WriteLine("[Broker] Subscriber unsubscribed.");
            }
            else
            {
                Console.WriteLine($"[Broker] Message from Sender: {message}");
                messageHistory.Enqueue(message); // păstrăm mesajul

                foreach (var sub in subscribers.Values)
                {
                    if (MessageMatches(sub, message))
                    {
                        try { sub.Socket.Send(Encoding.UTF8.GetBytes(message)); }
                        catch { }
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Broker] Error: {ex.Message}");
        }
        finally
        {
            if (!subscribers.ContainsKey(client))
            {
                try { client.Close(); } catch { }
            }
        }
    }

    static bool MessageMatches(SubscriberInfo sub, string message)
    {
        try
        {
            using var doc = JsonDocument.Parse(message);
            string type = doc.RootElement.GetProperty("Type").GetString();
            foreach (var tag in sub.Tags)
                if (type.Equals(tag, StringComparison.OrdinalIgnoreCase))
                    return true;
        }
        catch { }
        return false;
    }
}

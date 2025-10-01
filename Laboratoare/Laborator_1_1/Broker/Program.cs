using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Text.Json;

class SubscriberInfo
{
    public IPEndPoint Endpoint { get; set; }
    public string[] Tags { get; set; }
}

class Broker
{
    static ConcurrentDictionary<int, SubscriberInfo> subscribers = new ConcurrentDictionary<int, SubscriberInfo>();
    static ConcurrentQueue<string> allMessages = new ConcurrentQueue<string>();

    static void Main()
    {
        IPEndPoint endPoint = new IPEndPoint(IPAddress.Loopback, 5001);
        Socket listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        listener.Bind(endPoint);
        listener.Listen(20);

        Console.WriteLine("Broker started...");

        while (true)
        {
            Socket clientSocket = listener.Accept();
            ThreadPool.QueueUserWorkItem(HandleClient, clientSocket);
        }
    }

    static void HandleClient(object obj)
    {
        Socket client = (Socket)obj;
        try
        {
            byte[] buffer = new byte[2048];
            int received = client.Receive(buffer);
            string message = Encoding.UTF8.GetString(buffer, 0, received);

            if (message.StartsWith("SUBSCRIBE"))
            {
                // Format: SUBSCRIBE:port:tag1,tag2
                string[] parts = message.Split(':');
                int port = int.Parse(parts[1]);
                string[] tags = parts[2].Split(',');

                var sub = new SubscriberInfo
                {
                    Endpoint = new IPEndPoint(IPAddress.Loopback, port),
                    Tags = tags
                };

                subscribers[port] = sub;
                Console.WriteLine($"[Broker] New subscriber on port {port} for tags: {string.Join(",", tags)}");

                foreach (var msg in allMessages)
                {
                    TryForward(sub, msg);
                }
            }
            else if (message.StartsWith("UNSUBSCRIBE"))
            {
                // Format: UNSUBSCRIBE:port
                string[] parts = message.Split(':');
                int port = int.Parse(parts[1]);
                if (subscribers.TryRemove(port, out _))
                {
                    Console.WriteLine($"[Broker] Subscriber on port {port} removed.");
                }
            }
            else
            {
                Console.WriteLine($"[Broker] Received message from Sender: {message}");
                allMessages.Enqueue(message);

                foreach (var sub in subscribers.Values)
                {
                    TryForward(sub, message);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Broker] Error: {ex.Message}");
        }
        finally
        {
            client.Shutdown(SocketShutdown.Both);
            client.Close();
        }
    }

    static void TryForward(SubscriberInfo sub, string message)
    {
        try
        {
            if (!MessageMatches(sub, message))
                return;

            Socket s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            s.Connect(sub.Endpoint);
            s.Send(Encoding.UTF8.GetBytes(message));
            s.Shutdown(SocketShutdown.Both);
            s.Close();
            Console.WriteLine($"[Broker] Forwarded to {sub.Endpoint}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Broker] Failed to forward: {ex.Message}");
        }
    }

    static bool MessageMatches(SubscriberInfo sub, string message)
    {
        try
        {
            using var doc = JsonDocument.Parse(message);
            string type = doc.RootElement.GetProperty("Type").GetString();

            foreach (var tag in sub.Tags)
            {
                if (type.Equals(tag, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
        }
        catch { }
        return false;
    }
}

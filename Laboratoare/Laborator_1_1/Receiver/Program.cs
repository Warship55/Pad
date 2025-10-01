using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Receiver
{
    static void Main()
    {
        Console.WriteLine("Introdu tagurile pe care vrei să le primești (ex: Info,Alert):");
        string tags = Console.ReadLine();

        Socket brokerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        brokerSocket.Connect(new IPEndPoint(IPAddress.Loopback, 5001));

        // Trimite subscripția
        string subscribeMessage = $"SUBSCRIBE:{tags}";
        brokerSocket.Send(Encoding.UTF8.GetBytes(subscribeMessage));
        Console.WriteLine($"[Receiver] Subscribed for tags: {tags}");

        // Ascultă mesaje într-un thread
        Thread listenThread = new Thread(() =>
        {
            byte[] buffer = new byte[2048];
            while (true)
            {
                try
                {
                    int received = brokerSocket.Receive(buffer);
                    if (received == 0) break;

                    string message = Encoding.UTF8.GetString(buffer, 0, received);
                    Console.WriteLine($"[Receiver] Got: {message}");
                }
                catch
                {
                    Console.WriteLine("[Receiver] Disconnected from Broker.");
                    break;
                }
            }
        });
        listenThread.IsBackground = true;
        listenThread.Start();

        Console.WriteLine("Apasă Enter pentru a ieși...");
        Console.ReadLine();

        try
        {
            string unsubscribeMessage = $"UNSUBSCRIBE";
            brokerSocket.Send(Encoding.UTF8.GetBytes(unsubscribeMessage));
        }
        catch { }

        brokerSocket.Shutdown(SocketShutdown.Both);
        brokerSocket.Close();
        Console.WriteLine("[Receiver] Closed.");
    }
}

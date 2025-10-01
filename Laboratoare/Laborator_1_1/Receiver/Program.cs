using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class Receiver
{
    static void Main()
    {
        int myPort = 6000;
        IPEndPoint endPoint = new IPEndPoint(IPAddress.Loopback, myPort);
        Socket listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        listener.Bind(endPoint);
        listener.Listen(10);

        Console.WriteLine($"Receiver started on port {myPort}");

        Console.WriteLine("Introdu tagurile de mesaje pe care vrei să le primești (separate prin virgulă):");
        string tags = Console.ReadLine(); // ex: Info,Alert

        // Trimite subscripția la Broker
        using (Socket brokerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
        {
            brokerSocket.Connect(new IPEndPoint(IPAddress.Loopback, 5001));
            string subscribeMessage = $"SUBSCRIBE:{myPort}:{tags}";
            brokerSocket.Send(Encoding.UTF8.GetBytes(subscribeMessage));
        }

        // Rulează un thread care ascultă mesaje
        Thread listenerThread = new Thread(() =>
        {
            while (true)
            {
                try
                {
                    Socket client = listener.Accept();
                    ThreadPool.QueueUserWorkItem(HandleMessage, client);
                }
                catch { break; }
            }
        });
        listenerThread.Start();

        // Așteaptă ca utilizatorul să apese Enter pentru ieșire
        Console.WriteLine("Apasă Enter pentru a închide Receiver-ul...");
        Console.ReadLine();

        // Trimite UNSUBSCRIBE
        using (Socket brokerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
        {
            brokerSocket.Connect(new IPEndPoint(IPAddress.Loopback, 5001));
            string unsubscribeMessage = $"UNSUBSCRIBE:{myPort}";
            brokerSocket.Send(Encoding.UTF8.GetBytes(unsubscribeMessage));
        }

        Console.WriteLine("Receiver unsubscribed și închis.");
        listener.Close();
    }

    static void HandleMessage(object obj)
    {
        try
        {
            Socket client = (Socket)obj;
            byte[] buffer = new byte[2048];
            int received = client.Receive(buffer);
            string message = Encoding.UTF8.GetString(buffer, 0, received);
            Console.WriteLine($"[Receiver] Got: {message}");
            client.Shutdown(SocketShutdown.Both);
            client.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Receiver] Error: {ex.Message}");
        }
    }
}

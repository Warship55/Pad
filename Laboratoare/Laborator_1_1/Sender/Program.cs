using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

class Sender
{
    static void Main()
    {
        try
        {
            IPEndPoint brokerEndpoint = new IPEndPoint(IPAddress.Loopback, 5001);
            Socket senderSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            senderSocket.Connect(brokerEndpoint);

            Console.WriteLine("Introdu tipul mesajului (ex: Info, Alert, Error):");
            string type = Console.ReadLine();

            Console.WriteLine("Introdu conținutul mesajului:");
            string content = Console.ReadLine();

            string message = $"{{ \"Type\": \"{type}\", \"Content\": \"{content}\" }}";
            byte[] buffer = Encoding.UTF8.GetBytes(message);

            senderSocket.Send(buffer);
            Console.WriteLine($"[Sender] Message sent to Broker: {message}");

            senderSocket.Shutdown(SocketShutdown.Both);
            senderSocket.Close();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Sender] Error: {ex.Message}");
        }
    }
}

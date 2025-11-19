using Cassandra;
using ISession = Cassandra.ISession;

public class CassandraService
{
     public ISession Session { get; }

     public CassandraService()
     {
          var cluster = Cluster.Builder()
              .AddContactPoint("127.0.0.1")
              .Build();

          Session = cluster.Connect("warehouse");
     }
}

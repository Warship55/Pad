using BrokerServer.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<BrokerServiceImpl>();
app.MapGet("/", () => "Broker gRPC server is running. Use a gRPC client.");

app.Run();

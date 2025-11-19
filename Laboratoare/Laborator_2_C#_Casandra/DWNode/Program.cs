using Cassandra;

var builder = WebApplication.CreateBuilder(args);

// Controllers with JSON+XML
builder.Services.AddControllers()
       .AddXmlSerializerFormatters();

// Cassandra (if using DB)
builder.Services.AddSingleton<CassandraService>();

// HttpClient for PUSH/PULL
builder.Services.AddHttpClient();

// Swagger for testing
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Swagger UI in development
if (app.Environment.IsDevelopment())
{
     app.UseSwagger();
     app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapControllers();

app.Run();

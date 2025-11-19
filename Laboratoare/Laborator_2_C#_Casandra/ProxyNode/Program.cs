using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers(); // enable controllers for proxy endpoints

// Register RedisCache service
builder.Services.AddSingleton<RedisCache>();

// Register Load Balancing service
builder.Services.AddSingleton<LBService>();

// Optional: Swagger/OpenAPI pentru testare
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
     app.UseSwagger();
     app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapControllers(); // Map controller endpoints

app.Run();

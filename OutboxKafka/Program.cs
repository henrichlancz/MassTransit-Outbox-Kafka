using Confluent.Kafka;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using OutboxKafka.Contracts;
using OutboxKafka.Data;
using OutboxKafka.Models;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

builder.Services.AddSwaggerGen();

builder.AddNpgsqlDbContext<OrdersDbContext>("ordersdb");

builder.Services.AddMassTransit(x =>
{
    x.AddEntityFrameworkOutbox<OrdersDbContext>(o =>
    {
        o.QueryDelay = TimeSpan.FromSeconds(30);
        o.UsePostgres();
        o.UseBusOutbox();
        o.DisableInboxCleanupService();
    });

    x.UsingInMemory((context, cfg) =>
    {
        cfg.ConfigureEndpoints(context);
    });

    x.AddRider(rider =>
    {
        var submittedTopic = builder.Configuration["Kafka:OrderSubmittedTopic"] ?? "order-submitted";

        var producerConfig = new ProducerConfig
        {
            Acks = Acks.All,
            MessageTimeoutMs = 5000,
            RequestTimeoutMs = 2000
        };

        rider.AddProducer<OrderCreated>(submittedTopic, producerConfig);

        rider.UsingKafka((context, k) =>
        {
            var bootstrapServers = builder.Configuration.GetConnectionString("kafka") ?? "localhost:9092";
            k.Host(bootstrapServers);
        });
    });
});

var app = builder.Build();

app.MapDefaultEndpoints();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<OrdersDbContext>();
    await db.Database.EnsureCreatedAsync();
}

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/orders", async (          
    CreateOrderRequest request,
    OrdersDbContext db,
    ITopicProducer<OrderCreated> topicProducer,
    CancellationToken cancellationToken) =>
{
    var order = new Order
    {
        Id = Guid.NewGuid(),
        CustomerEmail = request.CustomerEmail,
        Total = request.Total,
        CreatedUtc = DateTime.UtcNow
    };

    db.Orders.Add(order);

    // Persist both state and message atomically via EF + MassTransit bus outbox.
    await topicProducer.Produce(new OrderCreated(
        order.Id,
        order.CustomerEmail,
        order.Total,
        order.CreatedUtc), cancellationToken);

    await db.SaveChangesAsync(cancellationToken);

    return Results.Accepted($"/orders/{order.Id}", new { order.Id });
});

app.MapGet("/orders", async (OrdersDbContext db, CancellationToken cancellationToken) =>
{
    var orders = await db.Orders
        .OrderByDescending(x => x.CreatedUtc)
        .Take(100)
        .ToListAsync(cancellationToken);

    return Results.Ok(orders);
});

app.Run();

public record CreateOrderRequest(string CustomerEmail, decimal Total);
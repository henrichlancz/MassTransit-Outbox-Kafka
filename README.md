MassTransit Kafka Outbox Demo (MS Aspire)
This repository demonstrates how to implement the Transactional Outbox Pattern using MassTransit 9.1+ and Kafka, orchestrated with .NET 10 and MS Aspire.

The Outbox pattern ensures at-least-once delivery by saving your business data and your outgoing message in the same database transaction. A background worker then ensures the message is successfully published to Kafka.

🚀 Features
.NET 10 & MassTransit 9.1: Leveraging the latest native support for Kafka Outbox.

MS Aspire: Simplified local orchestration of containers and service discovery.

PostgreSQL: Used as the backing store for both business data and the MassTransit Outbox.

Kafka & UI: Full event-streaming setup with a visual interface for message inspection.

🏗️ Architecture Overview
The API starts a PostgreSQL transaction.

It saves a business entity (e.g., Order) to the DB.

MassTransit inserts the outgoing event into a specialized OutboxMessage table within the same transaction.

Once the transaction commits, the MassTransit Outbox Delivery Service asynchronously picks up the message and pushes it to Kafka.

If Kafka is down, the message stays safely in the DB until the broker is back online.

💻 Getting Started
Prerequisites
.NET 10 SDK

Docker Desktop or Podman

Visual Studio 2022 (Preview) or VS Code

Running the Project
Clone the repository: git clone https://github.com/your-username/masstransit-kafka-outbox.git
cd masstransit-kafka-outbox
Run the Aspire AppHost:
dotnet run --project src/MyProject.AppHost
    ```
3.  Open the **Aspire Dashboard** URL provided in the terminal to monitor logs and access the Kafka-UI or pgAdmin.

---

## ⚙️ Configuration Snippet

In MassTransit 9.1, the configuration is simplified. Here is the core setup used in this demo:

```csharp
builder.Services.AddMassTransit(x =>
{
    // Configure PostgreSQL Outbox
    x.AddEntityFrameworkOutbox<OrdersDbContext>(o =>
    {
        o.UsePostgres();
        o.UseBusOutbox();
    });

    x.UsingInMemory((context, cfg) => cfg.ConfigureEndpoints(context));

    // Native Kafka Outbox integration
    x.AddRider(rider =>
    {
        rider.AddProducer<string, OrderCreatedEvent>("order-topic");
        rider.UsingKafka((context, k) =>
        {
            k.Host("localhost:9092");
        });
    });
});

📝 License
Distributed under the MIT License. See LICENSE for more information.

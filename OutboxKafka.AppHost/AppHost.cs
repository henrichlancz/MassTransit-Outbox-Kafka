var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres")
                      .WithDataVolume()
                      .WithPgAdmin(PgAdmin =>
                      {
                          PgAdmin.WithHostPort(5050);
                      });

var db = postgres.AddDatabase("ordersdb");

var kafka = builder.AddKafka("kafka")
                   .WithKafkaUI(kafkaUI => kafkaUI.WithHostPort(9100))
                   .WithDataBindMount("./kafka-data");

builder.AddProject<Projects.OutboxKafka>("outboxkafka")
    .WithReference(db)
    .WithReference(kafka)
    .WaitFor(db);

builder.Build().Run();



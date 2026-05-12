namespace OutboxKafka.Contracts;

public record OrderCreated(Guid OrderId, string CustomerEmail, decimal Total, DateTime CreatedUtc);

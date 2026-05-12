namespace OutboxKafka.Contracts;

public record OrderSubmitted(Guid OrderId, string CustomerEmail, decimal Total, DateTime CreatedUtc);

namespace OutboxKafka.Models;

public class Order
{
    public Guid Id { get; set; }
    public string CustomerEmail { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public DateTime CreatedUtc { get; set; }
}

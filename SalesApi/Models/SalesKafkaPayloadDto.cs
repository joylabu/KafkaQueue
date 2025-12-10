namespace SalesApi.Models
{
    public class SalesData
    {
        public int Id { get; set; }
        public string ProductName { get; set; }
        public int Quantity { get; set; }
        public decimal Price { get; set; }
        // Add other properties as needed
    }

    public class SalesKafkaPayloadDto
    {
        public required SalesData Data { get; set; }
    }

}

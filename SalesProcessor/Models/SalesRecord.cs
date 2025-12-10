namespace SalesProcessor.Models;

public class SalesRecord
{
    public int Id { get; set; }
    public string Payload { get; set; }

      public SalesRecord()
    {
        Payload = string.Empty; // or any default value
    }
}


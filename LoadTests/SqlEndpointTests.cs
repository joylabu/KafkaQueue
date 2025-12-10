using System.Net.Http.Json;
using FluentAssertions;

public class SqlEndpointTests
{
    private readonly HttpClient _client;

    public SqlEndpointTests()
    {
        _client = new HttpClient
        {
            BaseAddress = new Uri("http://localhost:5138")   // <- API must be running
        };
    }

    [Fact]
    public async Task PostSales_ShouldInsertIntoDatabase()
    {
        var payload = new
        {
            id = Guid.NewGuid().ToString(),
            amount = 20,
            items = new[] { "A", "B" }
        };

        var response = await _client.PostAsJsonAsync("/api/sales/postsales", payload);

        response.IsSuccessStatusCode.Should().BeTrue();
    }
}

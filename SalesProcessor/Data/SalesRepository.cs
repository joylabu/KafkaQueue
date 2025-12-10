using Microsoft.Data.SqlClient;
using SalesProcessor.Models;

namespace SalesProcessor.Data
{
public class SalesRepository
{
    private readonly string _connStr;

    public SalesRepository(string connStr)
    {
        _connStr = connStr;
    }

    public async Task<List<SalesRecord>> LoadPendingSales()
    {
        var list = new List<SalesRecord>();

        using var conn = new SqlConnection(_connStr);
        using var cmd = new SqlCommand(
            "SELECT Id, Payload FROM SalesPayload WHERE Status = 'Pending'", conn);

        await conn.OpenAsync();
        using var reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            list.Add(new SalesRecord
            {
                Id = reader.GetInt32(0),
                Payload = reader.GetString(1)
            });
        }

        return list;
    }

    public async Task UpdateStatus(int id, string status)
    {
        using var conn = new SqlConnection(_connStr);
        using var cmd = new SqlCommand(
            "UPDATE SalesPayload SET Status = @Status WHERE Id = @Id", conn);

        cmd.Parameters.AddWithValue("@Status", status);
        cmd.Parameters.AddWithValue("@Id", id);

        await conn.OpenAsync();
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task UpdateError(int id, string message)
    {
        using var conn = new SqlConnection(_connStr);
        using var cmd = new SqlCommand(
            "UPDATE SalesPayload SET Status = 'Error', ErrorMessage = @Err WHERE Id = @Id", conn);

        cmd.Parameters.AddWithValue("@Err", message);
        cmd.Parameters.AddWithValue("@Id", id);

        await conn.OpenAsync();
        await cmd.ExecuteNonQueryAsync();
    }

        //kafka
        public async Task InsertPendingAsync(string json)
        {
            using var conn = new SqlConnection(_connStr);

            using var cmd = new SqlCommand(
                "INSERT INTO SalesPayload (Payload, Status) VALUES (@Payload, 'Pending')",
                conn);

            cmd.Parameters.AddWithValue("@Payload", json);

            await conn.OpenAsync();
            await cmd.ExecuteNonQueryAsync();
        }

    }

}
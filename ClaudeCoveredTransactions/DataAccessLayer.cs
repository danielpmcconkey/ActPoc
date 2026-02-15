using Npgsql;

namespace ClaudeCoveredTransactions;

public static class DataAccessLayer
{
    public static NpgsqlConnection GetConnection()
    {
        string connectionString = GetConnectionString();
        return new NpgsqlConnection(connectionString);
    }
    public static string GetConnectionString()
    {
        string? pgPassHex = Environment.GetEnvironmentVariable("PGPASS");
        if(pgPassHex == null) throw new InvalidDataException("PGPASS environment variable not found");
        var converted = Convert.FromHexString(pgPassHex);
        string passNew = System.Text .Encoding.Unicode.GetString(converted);
            
        var connectionString = $"Host=localhost;Username=dansdev;Password='{passNew}';Database=atc;" +
                               "Timeout=15;Command Timeout=300;";
        return connectionString;
    }

    
}
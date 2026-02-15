using Npgsql;

namespace ClaudeCoveredTransactions;

public static class DataLakeReader
{
    public static List<Account> GetAccountsByAsOf(DateTime asOf)
    {
        var connectionString = DataAccessLayer.GetConnectionString();
        using var conn = DataAccessLayer.GetConnection();
        conn.Open();
        string q = @"select 
                       account_id, customer_id, account_type, account_status, open_date, current_balance,
                       interest_rate, credit_limit, apr, as_of
                   FROM public.accounts where as_of = @AsOf
                   ";
        using var cmd = new NpgsqlCommand(q, conn);
        cmd.Parameters.AddWithValue("@AsOf", DateTime.Parse("2024-10-01"));
        var reader = cmd.ExecuteReader();
        List<Account> accounts = new List<Account>();
        while (reader.Read())
        {
            var account_id =reader.GetInt32(0);
            var customer_id =reader.GetInt32(1);
            var account_type =reader.GetString(2);
            var account_status =reader.GetString(3);
            var open_date =reader.GetDateTime(4);
            var current_balance =reader.GetDecimal(5);
            decimal? interest_rate = reader.IsDBNull(6) ? null : reader.GetDecimal(6);
            decimal? credit_limit = reader.IsDBNull(7) ? null : reader.GetDecimal(7);
            decimal? apr = reader.IsDBNull(8) ? null : reader.GetDecimal(8);
            var as_of =reader.GetDateTime(9);
            
            accounts.Add(new Account(account_id, customer_id, account_type, account_status, open_date, current_balance,
                interest_rate, credit_limit, apr, as_of));
        }
        return accounts;
    }
}
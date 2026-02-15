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
        cmd.Parameters.AddWithValue("@AsOf", asOf);
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
    
    public static List<Address> GetAddressesByAsOf(DateTime asOf)
    {
        using var conn = DataAccessLayer.GetConnection();
        conn.Open();
        string q = @"select 
                       address_id, customer_id, address_line1, city, state_province, 
                       postal_code, country, start_date, end_date, as_of
                   FROM public.addresses where as_of = @AsOf
                   ";
        using var cmd = new NpgsqlCommand(q, conn);
        cmd.Parameters.AddWithValue("@AsOf", asOf);
        var reader = cmd.ExecuteReader();
        List<Address> addresses = new List<Address>();
        while (reader.Read())
        {
            var address_id = reader.GetInt32(0);
            var customer_id = reader.GetInt32(1);
            var address_line1 = reader.GetString(2);
            var city = reader.GetString(3);
            var state_province = reader.GetString(4);
            var postal_code = reader.GetString(5);
            var country = reader.GetString(6);
            var start_date = reader.GetDateTime(7);
            DateTime? end_date = reader.IsDBNull(8) ? null : reader.GetDateTime(8);
            var as_of = reader.GetDateTime(9);
            
            addresses.Add(new Address(address_id, customer_id, address_line1, city, state_province,
                postal_code, country, start_date, end_date, as_of));
        }
        return addresses;
    }

    public static List<Customer> GetCustomersByAsOf(DateTime asOf)
    {
        using var conn = DataAccessLayer.GetConnection();
        conn.Open();
        string q = @"select 
                       id, prefix, first_name, last_name, sort_name, suffix, 
                       birthdate, as_of
                   FROM public.customers where as_of = @AsOf
                   ";
        using var cmd = new NpgsqlCommand(q, conn);
        cmd.Parameters.AddWithValue("@AsOf", asOf);
        var reader = cmd.ExecuteReader();
        List<Customer> customers = new List<Customer>();
        while (reader.Read())
        {
            var id = reader.GetInt32(0);
            string? prefix = reader.IsDBNull(1) ? null : reader.GetString(1);
            var first_name = reader.GetString(2);
            var last_name = reader.GetString(3);
            var sort_name = reader.GetString(4);
            string? suffix = reader.IsDBNull(5) ? null : reader.GetString(5);
            var birthdate = reader.GetDateTime(6);
            var as_of = reader.GetDateTime(7);
            
            customers.Add(new Customer(id, prefix, first_name, last_name, sort_name, suffix,
                birthdate, as_of));
        }
        return customers;
    }

    public static List<CustomerSegment> GetCustomerSegmentsByAsOf(DateTime asOf)
    {
        using var conn = DataAccessLayer.GetConnection();
        conn.Open();
        string q = @"select 
                       id, customer_id, segment_id, as_of
                   FROM public.customers_segments where as_of = @AsOf
                   ";
        using var cmd = new NpgsqlCommand(q, conn);
        cmd.Parameters.AddWithValue("@AsOf", asOf);
        var reader = cmd.ExecuteReader();
        List<CustomerSegment> customerSegments = new List<CustomerSegment>();
        while (reader.Read())
        {
            var id = reader.GetInt32(0);
            var customer_id = reader.GetInt32(1);
            var segment_id = reader.GetInt32(2);
            var as_of = reader.GetDateTime(3);
            
            customerSegments.Add(new CustomerSegment(id, customer_id, segment_id, as_of));
        }
        return customerSegments;
    }

    public static List<Segment> GetSegmentsByAsOf(DateTime asOf)
    {
        using var conn = DataAccessLayer.GetConnection();
        conn.Open();
        string q = @"select 
                       segment_id, segment_name, segment_code, as_of
                   FROM public.segments where as_of = @AsOf
                   ";
        using var cmd = new NpgsqlCommand(q, conn);
        cmd.Parameters.AddWithValue("@AsOf", asOf);
        var reader = cmd.ExecuteReader();
        List<Segment> segments = new List<Segment>();
        while (reader.Read())
        {
            var segment_id = reader.GetInt32(0);
            var segment_name = reader.GetString(1);
            var segment_code = reader.GetString(2);
            var as_of = reader.GetDateTime(3);
            
            segments.Add(new Segment(segment_id, segment_name, segment_code, as_of));
        }
        return segments;
    }

    public static List<Transaction> GetTransactionsByAsOf(DateTime asOf)
    {
        using var conn = DataAccessLayer.GetConnection();
        conn.Open();
        string q = @"select 
                       transaction_id, account_id, txn_timestamp, txn_type, amount,
                       description, as_of
                   FROM public.transactions where as_of = @AsOf
                   ";
        using var cmd = new NpgsqlCommand(q, conn);
        cmd.Parameters.AddWithValue("@AsOf", asOf);
        var reader = cmd.ExecuteReader();
        List<Transaction> transactions = new List<Transaction>();
        while (reader.Read())
        {
            var transaction_id = reader.GetInt32(0);
            var account_id = reader.GetInt32(1);
            var txn_timestamp = reader.GetDateTime(2);
            var txn_type = reader.GetString(3);
            var amount = reader.GetDecimal(4);
            string? description = reader.IsDBNull(5) ? null : reader.GetString(5);
            var as_of = reader.GetDateTime(6);
            
            transactions.Add(new Transaction(transaction_id, account_id, txn_timestamp, txn_type, amount,
                description, as_of));
        }
        return transactions;
    }

}
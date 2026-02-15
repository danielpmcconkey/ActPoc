namespace ClaudeCoveredTransactions;


public record Account (int account_id, int customer_id, string account_type, string account_status, DateTime open_date,
    decimal current_balance, decimal? interest_rate, decimal? credit_limit, decimal? apr, DateTime as_of);
public record Address (int address_id, int customer_id, string address_line1, string city, string state_province, 
    string postal_code, string country, DateTime start_date, DateTime? end_date, DateTime as_of);
public record Customer (int id, string? prefix, string first_name, string last_name, string sort_name, string? suffix, 
    DateTime birthdate, DateTime as_of);
public record CustomerSegment (int id, int customer_id, int segment_id, DateTime as_of);
public record Segment (int segment_id, string segment_name, string segment_code, DateTime as_of);
public record Transaction (int transaction_id, int account_id, DateTime txn_timestamp, string txn_type, decimal amount,
    string? description, DateTime as_of);


namespace SharedFunctions;

public record Account (int account_id, int customer_id, string account_type, string account_status, DateTime open_date,
    decimal current_balance, decimal? interest_rate, decimal? credit_limit, decimal? apr);
public record Address (int address_id, int customer_id, string address_line1, string city, string state_province, 
    string postal_code, string country, DateTime start_date, DateTime? end_date);
public record Customer (int id, string? prefix, string first_name, string last_name, string sort_name, string? suffix, 
    DateTime birthdate);
public record CustomerSegment (int id, int customer_id, int segment_id);
public record Segment (int segment_id, string segment_name, string segment_code);
public record Transaction (int transaction_id, int account_id, DateTime txn_timestamp, string txn_type, decimal amount,
    string? description);

public record CustomerAddressChange (string change_type, int address_id, int customer_id, string customer_name, 
    string address_line1, string city, string state_province, string postal_code, string country, DateTime start_date,
    DateTime? end_date);




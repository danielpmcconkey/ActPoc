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

// Implements Functional Spec Section 3.1 — 22-field denormalized output record
public record CoveredTransaction(
    int transaction_id,        // Pos 1:  transactions.transaction_id (INTEGER, unquoted)
    DateTime txn_timestamp,    // Pos 2:  transactions.txn_timestamp (TIMESTAMP, quoted YYYY-MM-DD HH:MM:SS)
    string txn_type,           // Pos 3:  transactions.txn_type (VARCHAR, quoted)
    decimal amount,            // Pos 4:  transactions.amount (DECIMAL, quoted, 2dp)
    string? description,       // Pos 5:  transactions.description (VARCHAR, quoted; NULL -> unquoted NULL)
    int customer_id,           // Pos 6:  accounts.customer_id (INTEGER, unquoted)
    string? name_prefix,       // Pos 7:  customers.prefix RENAMED (VARCHAR, quoted; NULL -> unquoted NULL)
    string first_name,         // Pos 8:  customers.first_name (VARCHAR, quoted)
    string last_name,          // Pos 9:  customers.last_name (VARCHAR, quoted)
    string sort_name,          // Pos 10: customers.sort_name (VARCHAR, quoted)
    string? name_suffix,       // Pos 11: customers.suffix RENAMED (VARCHAR, quoted; NULL -> unquoted NULL)
    string customer_segment,   // Pos 12: segments.segment_code RENAMED (VARCHAR, quoted; via join BR-6/BR-7)
    int address_id,            // Pos 13: addresses.address_id (INTEGER, unquoted)
    string address_line1,      // Pos 14: addresses.address_line1 (VARCHAR, quoted)
    string city,               // Pos 15: addresses.city (VARCHAR, quoted)
    string state_province,     // Pos 16: addresses.state_province (VARCHAR, quoted)
    string postal_code,        // Pos 17: addresses.postal_code (VARCHAR, quoted)
    string country,            // Pos 18: addresses.country (VARCHAR, quoted)
    int account_id,            // Pos 19: accounts.account_id (INTEGER, unquoted)
    string account_type,       // Pos 20: accounts.account_type (VARCHAR, quoted)
    string account_status,     // Pos 21: accounts.account_status (VARCHAR, quoted)
    DateTime account_opened    // Pos 22: accounts.open_date RENAMED (DATE, quoted YYYY-MM-DD)
);


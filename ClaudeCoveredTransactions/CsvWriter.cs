using System.Text;

namespace ClaudeCoveredTransactions;

// Implements Functional Spec Section 4.7 — Project, Sort, and Write Output
// Implements Functional Spec Sections 3.2, 3.3, 3.4
// Tested by: TC-24 through TC-34, TC-40 through TC-43

public static class CsvWriter
{
    // Implements Spec Section 3.1 — 22 output field names in exact order
    private static readonly string[] HeaderFields =
    {
        "transaction_id", "txn_timestamp", "txn_type", "amount", "description",
        "customer_id", "name_prefix", "first_name", "last_name", "sort_name",
        "name_suffix", "customer_segment", "address_id", "address_line1", "city",
        "state_province", "postal_code", "country", "account_id", "account_type",
        "account_status", "account_opened"
    };

    public static void Write(List<CoveredTransaction> records, string outputDirectory, DateTime effectiveDate)
    {
        // Implements Spec Section 1.3 — file naming: covered_transactions_<YYYYMMDD>.csv
        string fileName = $"covered_transactions_{effectiveDate:yyyyMMdd}.csv";
        string finalPath = Path.Combine(outputDirectory, fileName);

        // Implements Spec Section 6.4 — Atomic write: write to temp file, then rename
        string tempPath = finalPath + ".tmp";

        try
        {
            using (var writer = new StreamWriter(tempPath, false, new UTF8Encoding(false)))
            {
                // Implements Spec Section 3.2 — Header row: all 22 field names double-quoted (BR-13)
                writer.WriteLine(string.Join(",", HeaderFields.Select(f => $"\"{f}\"")));

                // Implements Spec Section 3.2 — Data rows with quoting per Section 3.3
                foreach (var r in records)
                {
                    writer.WriteLine(FormatRow(r));
                }

                // Implements Spec Section 3.2 — Blank line after data rows (BR-13)
                writer.WriteLine();

                // Implements Spec Section 3.2 — Footer: "Expected records: N" (BR-13)
                writer.Write($"Expected records: {records.Count}");
            }

            // Implements Spec Section 6.4 — Atomic rename (overwrite if exists for idempotency per Spec 7.2)
            File.Move(tempPath, finalPath, overwrite: true);
        }
        catch
        {
            // Implements Spec Section 6.4 — Clean up temp file on failure
            if (File.Exists(tempPath))
                File.Delete(tempPath);
            throw;
        }
    }

    // Implements Spec Section 3.3 — Quoting and Formatting Rules
    //
    // INTEGER columns (transaction_id, customer_id, address_id, account_id): unquoted
    // All other columns: double-quoted
    // NULL values: literal unquoted NULL regardless of source type (BR-15)
    // amount: exactly 2 decimal places
    // txn_timestamp: YYYY-MM-DD HH:MM:SS
    // account_opened: YYYY-MM-DD
    private static string FormatRow(CoveredTransaction r)
    {
        var fields = new string[22];

        // Pos 1: transaction_id — INTEGER, unquoted
        fields[0] = r.transaction_id.ToString();

        // Pos 2: txn_timestamp — TIMESTAMP, quoted, YYYY-MM-DD HH:MM:SS
        fields[1] = Quote(r.txn_timestamp.ToString("yyyy-MM-dd HH:mm:ss"));

        // Pos 3: txn_type — VARCHAR, quoted
        fields[2] = Quote(r.txn_type);

        // Pos 4: amount — DECIMAL, quoted, exactly 2 decimal places
        fields[3] = Quote(r.amount.ToString("F2"));

        // Pos 5: description — VARCHAR, quoted; NULL -> unquoted NULL (BR-15)
        fields[4] = FormatNullable(r.description);

        // Pos 6: customer_id — INTEGER, unquoted
        fields[5] = r.customer_id.ToString();

        // Pos 7: name_prefix — VARCHAR, quoted; NULL -> unquoted NULL (BR-15)
        fields[6] = FormatNullable(r.name_prefix);

        // Pos 8: first_name — VARCHAR, quoted
        fields[7] = Quote(r.first_name);

        // Pos 9: last_name — VARCHAR, quoted
        fields[8] = Quote(r.last_name);

        // Pos 10: sort_name — VARCHAR, quoted
        fields[9] = Quote(r.sort_name);

        // Pos 11: name_suffix — VARCHAR, quoted; NULL -> unquoted NULL (BR-15)
        fields[10] = FormatNullable(r.name_suffix);

        // Pos 12: customer_segment — VARCHAR, quoted
        fields[11] = Quote(r.customer_segment);

        // Pos 13: address_id — INTEGER, unquoted
        fields[12] = r.address_id.ToString();

        // Pos 14: address_line1 — VARCHAR, quoted
        fields[13] = Quote(r.address_line1);

        // Pos 15: city — VARCHAR, quoted
        fields[14] = Quote(r.city);

        // Pos 16: state_province — VARCHAR, quoted
        fields[15] = Quote(r.state_province);

        // Pos 17: postal_code — VARCHAR, quoted
        fields[16] = Quote(r.postal_code);

        // Pos 18: country — VARCHAR, quoted
        fields[17] = Quote(r.country);

        // Pos 19: account_id — INTEGER, unquoted
        fields[18] = r.account_id.ToString();

        // Pos 20: account_type — VARCHAR, quoted
        fields[19] = Quote(r.account_type);

        // Pos 21: account_status — VARCHAR, quoted
        fields[20] = Quote(r.account_status);

        // Pos 22: account_opened — DATE, quoted, YYYY-MM-DD
        fields[21] = Quote(r.account_opened.ToString("yyyy-MM-dd"));

        return string.Join(",", fields);
    }

    private static string Quote(string value) => $"\"{value}\"";

    // Implements BR-15: NULL -> literal unquoted NULL; non-NULL -> quoted
    private static string FormatNullable(string? value) =>
        value == null ? "NULL" : Quote(value);
}

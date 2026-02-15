namespace ClaudeCoveredTransactions;

// Implements Functional Spec Section 4.1 — Resolve Snapshots (BR-12, BR-17)
// Tested by: TC-21, TC-22, TC-23, TC-35
//
// For each of the 6 input tables, determines the as_of date to use:
//   1. Use effective_date if a snapshot exists for that date.
//   2. Otherwise, fall back to max(as_of) WHERE as_of <= effective_date.
//   3. If no snapshot exists at or before effective_date, this is an error (Spec Section 6.2).

public class ResolvedSnapshots
{
    public DateTime Transactions { get; init; }
    public DateTime Accounts { get; init; }
    public DateTime Customers { get; init; }
    public DateTime Addresses { get; init; }
    public DateTime CustomersSegments { get; init; }
    public DateTime Segments { get; init; }
}

public static class SnapshotResolver
{
    private static readonly string[] RequiredTables =
    {
        "transactions",
        "accounts",
        "customers",
        "addresses",
        "customers_segments",
        "segments"
    };

    public static ResolvedSnapshots Resolve(DateTime effectiveDate)
    {
        var resolved = new Dictionary<string, DateTime>();

        foreach (var table in RequiredTables)
        {
            // Implements BR-12: max(as_of) WHERE as_of <= effective_date
            DateTime? snapshotDate = DataLakeReader.ResolveSnapshotDate(table, effectiveDate);

            if (snapshotDate == null)
            {
                // Implements Functional Spec Section 6.2 — no snapshot available
                throw new InvalidOperationException(
                    $"No snapshot available for table '{table}' at or before effective date {effectiveDate:yyyy-MM-dd}. " +
                    $"Cannot proceed.");
            }

            resolved[table] = snapshotDate.Value;
        }

        return new ResolvedSnapshots
        {
            Transactions = resolved["transactions"],
            Accounts = resolved["accounts"],
            Customers = resolved["customers"],
            Addresses = resolved["addresses"],
            CustomersSegments = resolved["customers_segments"],
            Segments = resolved["segments"]
        };
    }
}

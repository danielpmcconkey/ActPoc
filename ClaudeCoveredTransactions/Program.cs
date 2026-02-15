using System.Globalization;

namespace ClaudeCoveredTransactions;

// Implements Functional Spec Sections 1.2, 6.1, 6.2, 6.4 — Entry point and orchestration
// Tested by: TC-45 through TC-50 (error handling), TC-34 (file naming)
//
// Invocation: <program> <output-directory> <effective-date>
//   output-directory: existing, writable directory
//   effective-date:   YYYYMMDD format, valid calendar date

class Program
{
    static int Main(string[] args)
    {
        try
        {
            // ──────────────────────────────────────────────────────────────────
            // Step 0: INPUT VALIDATION (Spec Section 6.1, BR-1)
            // Tested by: TC-45, TC-46, TC-47, TC-48, TC-49
            // ──────────────────────────────────────────────────────────────────

            if (args.Length < 1)
            {
                // Implements Spec Section 6.1 — No arguments provided
                Console.Error.WriteLine("Error: No arguments provided.");
                Console.Error.WriteLine("Usage: ClaudeCoveredTransactions <output-directory> <effective-date>");
                Console.Error.WriteLine("  output-directory: Path to an existing directory for output CSV");
                Console.Error.WriteLine("  effective-date:   Processing date in YYYYMMDD format");
                return 1;
            }

            if (args.Length < 2)
            {
                // Implements Spec Section 6.1 — Missing effective-date argument
                Console.Error.WriteLine("Error: Missing effective-date argument.");
                Console.Error.WriteLine("Usage: ClaudeCoveredTransactions <output-directory> <effective-date>");
                return 1;
            }

            string outputDirectory = args[0];
            string effectiveDateArg = args[1];

            // Implements Spec Section 6.1 — Output directory must exist
            if (!Directory.Exists(outputDirectory))
            {
                Console.Error.WriteLine($"Error: Output directory does not exist: {outputDirectory}");
                return 1;
            }

            // Implements Spec Section 6.1 — Effective date must be exactly 8 digits, YYYYMMDD format
            if (effectiveDateArg.Length != 8 || !effectiveDateArg.All(char.IsDigit))
            {
                Console.Error.WriteLine($"Error: Effective date must be in YYYYMMDD format (8 digits). Got: {effectiveDateArg}");
                return 1;
            }

            // Implements Spec Section 6.1 — Effective date must be a valid calendar date
            if (!DateTime.TryParseExact(effectiveDateArg, "yyyyMMdd", CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out DateTime effectiveDate))
            {
                Console.Error.WriteLine($"Error: Invalid calendar date: {effectiveDateArg}. " +
                                        "Must be a valid date in YYYYMMDD format (e.g., 20241001).");
                return 1;
            }

            Console.WriteLine($"Covered Transactions ETL — Effective Date: {effectiveDate:yyyy-MM-dd}");
            Console.WriteLine($"Output Directory: {outputDirectory}");

            // ──────────────────────────────────────────────────────────────────
            // Step 1: RESOLVE SNAPSHOTS (Spec Section 4.1, BR-12, BR-17)
            // Tested by: TC-21, TC-22, TC-23, TC-35
            // ──────────────────────────────────────────────────────────────────

            Console.WriteLine("Step 1: Resolving snapshots...");
            ResolvedSnapshots snapshots = SnapshotResolver.Resolve(effectiveDate);

            Console.WriteLine($"  transactions:       {snapshots.Transactions:yyyy-MM-dd}");
            Console.WriteLine($"  accounts:           {snapshots.Accounts:yyyy-MM-dd}");
            Console.WriteLine($"  customers:          {snapshots.Customers:yyyy-MM-dd}");
            Console.WriteLine($"  addresses:          {snapshots.Addresses:yyyy-MM-dd}");
            Console.WriteLine($"  customers_segments: {snapshots.CustomersSegments:yyyy-MM-dd}");
            Console.WriteLine($"  segments:           {snapshots.Segments:yyyy-MM-dd}");

            // ──────────────────────────────────────────────────────────────────
            // Steps 2-6: TRANSFORMATION ENGINE (Spec Sections 4.2-4.6)
            // All filtering, joining, enrichment, and dedup in a single SQL query.
            // Tested by: TC-01 through TC-18, TC-36 through TC-39, TC-51, TC-52
            // ──────────────────────────────────────────────────────────────────

            Console.WriteLine("Steps 2-6: Executing transformation query...");
            List<CoveredTransaction> results = TransformationEngine.Execute(snapshots, effectiveDate);
            Console.WriteLine($"  Qualifying records: {results.Count}");

            // ──────────────────────────────────────────────────────────────────
            // Step 7: PROJECT, SORT, AND WRITE OUTPUT (Spec Section 4.7)
            // Sort is already applied in SQL (customer_id ASC, transaction_id DESC).
            // Tested by: TC-24 through TC-34, TC-40 through TC-43
            // ──────────────────────────────────────────────────────────────────

            Console.WriteLine("Step 7: Writing output CSV...");
            CsvWriter.Write(results, outputDirectory, effectiveDate);

            string outputFile = Path.Combine(outputDirectory,
                $"covered_transactions_{effectiveDate:yyyyMMdd}.csv");
            Console.WriteLine($"  Output: {outputFile}");
            Console.WriteLine("Complete.");

            return 0;
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("No snapshot available"))
        {
            // Implements Spec Section 6.2 — No snapshot available for a required table
            Console.Error.WriteLine($"Error: {ex.Message}");
            return 1;
        }
        catch (Npgsql.NpgsqlException ex)
        {
            // Implements Spec Section 6.2 — Database connection failure
            Console.Error.WriteLine($"Error: Database connection failure: {ex.Message}");
            return 1;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Error: Unexpected failure: {ex.Message}");
            return 1;
        }
    }
}

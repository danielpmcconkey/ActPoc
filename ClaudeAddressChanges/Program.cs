using System.Globalization;
using ClaudeAddressChanges.Engine;

// =============================================================================
// Address Changes ETL — Command-Line Entry Point
//
// Usage:  ClaudeAddressChanges <input-directory> <output-directory> <effective-date>
//
//   <input-directory>   Directory containing addresses_YYYYMMDD.csv and
//                       customers_YYYYMMDD.csv snapshot files.
//   <output-directory>  Directory where the output address_changes_YYYYMMDD.csv
//                       file will be written.
//   <effective-date>    The date to process, in YYYYMMDD format.
//                       The pipeline compares this date's snapshot against the
//                       previous calendar day's snapshot (effective-date minus 1).
//
// Exit codes:
//   0 = Success
//   1 = Error (message written to stderr)
//
// All log output goes to stderr; stdout is unused.
// =============================================================================

if (args.Length < 3)
{
    Console.Error.WriteLine("Usage: ClaudeAddressChanges <input-directory> <output-directory> <effective-date>");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  <input-directory>   Directory containing addresses_YYYYMMDD.csv and");
    Console.Error.WriteLine("                      customers_YYYYMMDD.csv snapshot files");
    Console.Error.WriteLine("  <output-directory>  Directory for output address_changes_YYYYMMDD.csv file");
    Console.Error.WriteLine("  <effective-date>    Processing date in YYYYMMDD format (e.g., 20241002)");
    Console.Error.WriteLine();
    Console.Error.WriteLine("The ETL compares the effective date's address snapshot against the");
    Console.Error.WriteLine("previous calendar day (effective-date minus 1 day).");
    return 1;
}

string inputDir = args[0];
string outputDir = args[1];
string dateArg = args[2];

if (!DateOnly.TryParseExact(dateArg, "yyyyMMdd", CultureInfo.InvariantCulture,
        DateTimeStyles.None, out var effectiveDate))
{
    Console.Error.WriteLine($"[ERROR] Invalid effective date: '{dateArg}'. Expected format: YYYYMMDD (e.g., 20241002)");
    return 1;
}

if (!Directory.Exists(inputDir))
{
    Console.Error.WriteLine($"[ERROR] Input directory does not exist: {inputDir}");
    return 1;
}

if (!Directory.Exists(outputDir))
{
    Console.Error.WriteLine($"[ERROR] Output directory does not exist: {outputDir}");
    return 1;
}

try
{
    var pipeline = new Pipeline(inputDir, outputDir, effectiveDate);
    pipeline.Run();
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[ERROR] ETL failed: {ex.Message}");
    return 1;
}

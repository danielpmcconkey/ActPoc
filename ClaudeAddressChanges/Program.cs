using ClaudeAddressChanges.Engine;

// =============================================================================
// Address Changes ETL — Command-Line Entry Point
//
// Usage:  ClaudeAddressChanges <input-directory> <output-directory>
//
//   <input-directory>   Directory containing addresses_YYYYMMDD.csv and
//                       customers_YYYYMMDD.csv snapshot files.
//   <output-directory>  Directory where address_changes_YYYYMMDD.csv files
//                       will be written.
//
// Exit codes:
//   0 = Success
//   1 = Error (message written to stderr)
//
// All log output goes to stderr; stdout is unused.
// =============================================================================

if (args.Length < 2)
{
    Console.Error.WriteLine("Usage: ClaudeAddressChanges <input-directory> <output-directory>");
    Console.Error.WriteLine();
    Console.Error.WriteLine("  <input-directory>   Directory containing addresses_YYYYMMDD.csv and");
    Console.Error.WriteLine("                      customers_YYYYMMDD.csv snapshot files");
    Console.Error.WriteLine("  <output-directory>  Directory for output address_changes_YYYYMMDD.csv files");
    return 1;
}

string inputDir = args[0];
string outputDir = args[1];

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
    var pipeline = new Pipeline(inputDir, outputDir);
    pipeline.Run();
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[ERROR] ETL failed: {ex.Message}");
    return 1;
}

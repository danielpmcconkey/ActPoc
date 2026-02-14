using System.Diagnostics;
using System.Globalization;
using ClaudeAddressChanges.IO;
using ClaudeAddressChanges.Models;
using ClaudeAddressChanges.Validation;

namespace ClaudeAddressChanges.Engine;

/// <summary>
/// ETL pipeline orchestrator.
/// Implements Section 5 (Functional Processing Flow):
///   5.1: Initialization — file discovery, date sorting, baseline designation
///   5.2: Daily processing loop — load, compare, enrich, write
///
/// Memory profile for maximum scale (10M addresses, 5M customers):
///   ~4GB per address dictionary × 2 dictionaries at peak = ~8GB
///   ~0.5GB for customer name lookup
///   Recommended: allocate 12GB+ heap for production runs.
/// </summary>
public sealed class Pipeline
{
    private readonly string _inputDir;
    private readonly string _outputDir;

    public Pipeline(string inputDir, string outputDir)
    {
        _inputDir = inputDir;
        _outputDir = outputDir;
    }

    /// <summary>
    /// Executes the full ETL pipeline.
    /// Section 7.1: Running multiple times with same input produces identical output (idempotent).
    /// Section 7.4: Dates are processed in strict chronological order.
    /// </summary>
    public void Run()
    {
        var totalTimer = Stopwatch.StartNew();

        Log("Starting Address Changes ETL");
        Log($"Input directory:  {_inputDir}");
        Log($"Output directory: {_outputDir}");

        // --- Section 5.1: Initialization ---

        // Step 1-2: Discover and sort address input dates
        var addressDates = DiscoverDates("addresses");
        Log($"Discovered {addressDates.Count} address snapshot(s): " +
            $"{addressDates.First():yyyyMMdd} to {addressDates.Last():yyyyMMdd}");

        // Step 4: Discover customer input dates
        var customerDates = DiscoverDates("customers");
        Log($"Discovered {customerDates.Count} customer snapshot(s)");

        // Step 5: Validate at least two address dates exist (BR-1 requires a baseline + at least one processing day)
        InputValidator.ValidateMinimumDates(addressDates);

        // Step 3: Designate first date as baseline
        var baseline = addressDates[0];
        Log($"Baseline date: {baseline:yyyyMMdd} (no output produced — BR-1)");

        // --- Section 5.2: Daily Processing Loop ---

        Dictionary<int, AddressRecord>? previousAddresses = null;
        DateOnly? lastResolvedCustomerDate = null;
        Dictionary<int, string>? customerNames = null;

        for (int i = 0; i < addressDates.Count; i++)
        {
            var currentDate = addressDates[i];
            var addressPath = GetFilePath("addresses", currentDate);

            if (i == 0)
            {
                // Implements BR-1: Baseline — load only, no output produced
                var stepTimer = Stopwatch.StartNew();
                previousAddresses = AddressFileReader.ReadFile(addressPath);
                stepTimer.Stop();
                Log($"Loaded baseline {currentDate:yyyyMMdd}: {previousAddresses.Count:N0} addresses ({stepTimer.ElapsedMilliseconds}ms)");
                continue;
            }

            Log($"--- Processing {currentDate:yyyyMMdd} ---");
            var dayTimer = Stopwatch.StartNew();

            // Step 7: Load current-day address snapshot
            var loadTimer = Stopwatch.StartNew();
            var currentAddresses = AddressFileReader.ReadFile(addressPath);
            loadTimer.Stop();
            Log($"  Loaded {currentAddresses.Count:N0} current-day addresses ({loadTimer.ElapsedMilliseconds}ms)");

            // Step 8: Resolve customer file per Section 2.3, Decision D-2 (Option B)
            var resolvedCustomerDate = ResolveCustomerDate(customerDates, currentDate);
            if (resolvedCustomerDate is null)
            {
                // Section 6.2: Halt if no customer file can be resolved
                throw new InvalidOperationException(
                    $"No customer file available for processing date {currentDate:yyyyMMdd}. " +
                    $"Searched for customers_YYYYMMDD.csv with date <= {currentDate:yyyyMMdd} (Section 6.2)");
            }

            // Step 9: Load customer names (cached if same file as previous iteration)
            if (resolvedCustomerDate != lastResolvedCustomerDate)
            {
                var custPath = GetFilePath("customers", resolvedCustomerDate.Value);
                var custTimer = Stopwatch.StartNew();
                customerNames = CustomerFileReader.ReadFile(custPath);
                custTimer.Stop();
                lastResolvedCustomerDate = resolvedCustomerDate;
                Log($"  Loaded customer file {resolvedCustomerDate.Value:yyyyMMdd}: {customerNames.Count:N0} customers ({custTimer.ElapsedMilliseconds}ms)");
            }
            else
            {
                Log($"  Reusing cached customer file {resolvedCustomerDate.Value:yyyyMMdd}");
            }

            // Steps 12-15: Change detection
            var detectTimer = Stopwatch.StartNew();
            var changes = ChangeDetector.DetectChanges(previousAddresses!, currentAddresses, customerNames!);
            detectTimer.Stop();

            int newCount = 0, updatedCount = 0, deletedCount = 0;
            foreach (var c in changes)
            {
                switch (c.ChangeType)
                {
                    case ChangeType.NEW: newCount++; break;
                    case ChangeType.UPDATED: updatedCount++; break;
                    case ChangeType.DELETED: deletedCount++; break;
                }
            }
            Log($"  Changes detected: {changes.Count:N0} (NEW={newCount:N0}, UPDATED={updatedCount:N0}, DELETED={deletedCount:N0}) ({detectTimer.ElapsedMilliseconds}ms)");

            // Step 16: Write output file
            // Implements BR-8: File is always produced, even with zero changes
            var outputPath = Path.Combine(_outputDir, $"address_changes_{currentDate:yyyyMMdd}.csv");
            var writeTimer = Stopwatch.StartNew();
            ChangeLogWriter.Write(outputPath, changes);
            writeTimer.Stop();
            Log($"  Output written: {outputPath} ({writeTimer.ElapsedMilliseconds}ms)");

            // Step 17: Current becomes previous for next iteration
            // The previous-day dictionary is released for GC once reassigned.
            previousAddresses = currentAddresses;

            dayTimer.Stop();
            Log($"  Day complete ({dayTimer.ElapsedMilliseconds}ms)");
        }

        totalTimer.Stop();
        Log($"ETL processing complete. Total elapsed: {totalTimer.Elapsed.TotalSeconds:F1}s");
    }

    /// <summary>
    /// Discovers all files matching the pattern {prefix}_YYYYMMDD.csv in the input directory,
    /// extracts dates, and returns them sorted ascending.
    /// Implements Section 5.1, Steps 1-2.
    /// </summary>
    private List<DateOnly> DiscoverDates(string prefix)
    {
        var pattern = $"{prefix}_*.csv";
        var files = Directory.GetFiles(_inputDir, pattern);
        var dates = new List<DateOnly>();

        int prefixLen = prefix.Length + 1; // "prefix_"

        foreach (var file in files)
        {
            var name = Path.GetFileNameWithoutExtension(file);

            if (name.Length < prefixLen + 8)
                continue; // filename too short to contain a date

            var datePart = name[prefixLen..];

            if (DateOnly.TryParseExact(datePart, "yyyyMMdd", CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out var date))
            {
                dates.Add(date);
            }
        }

        dates.Sort();
        return dates;
    }

    /// <summary>
    /// Resolves the customer file to use for a given processing date.
    /// Implements Section 2.3, Decision D-2 (Option B):
    ///   Use the most recent customer file whose date is &lt;= the current processing date.
    ///
    /// Uses binary search for O(log N) lookup on sorted customer dates.
    /// Implements BR-13: Customer file does not need to match the processing date exactly.
    /// </summary>
    private static DateOnly? ResolveCustomerDate(List<DateOnly> sortedDates, DateOnly currentDate)
    {
        if (sortedDates.Count == 0)
            return null;

        int index = sortedDates.BinarySearch(currentDate);

        if (index >= 0)
            return sortedDates[index]; // exact match

        // BinarySearch returns ~insertionPoint when not found
        int insertionPoint = ~index;

        if (insertionPoint == 0)
            return null; // all customer dates are after the current date

        return sortedDates[insertionPoint - 1]; // most recent date before current
    }

    /// <summary>
    /// Builds the full file path for a given prefix and date.
    /// E.g., prefix="addresses", date=2024-10-02 -> "{inputDir}/addresses_20241002.csv"
    /// </summary>
    private string GetFilePath(string prefix, DateOnly date)
    {
        var path = Path.Combine(_inputDir, $"{prefix}_{date:yyyyMMdd}.csv");

        // Section 6.1, Decision D-5: Halt on missing address file
        if (!File.Exists(path))
        {
            throw new InvalidOperationException(
                $"Expected input file does not exist: {path} (Section 6.1)");
        }

        return path;
    }

    private static void Log(string message)
    {
        Console.Error.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
    }
}

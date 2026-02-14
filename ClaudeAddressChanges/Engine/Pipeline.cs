using System.Diagnostics;
using System.Globalization;
using ClaudeAddressChanges.IO;
using ClaudeAddressChanges.Models;
using ClaudeAddressChanges.Validation;

namespace ClaudeAddressChanges.Engine;

/// <summary>
/// ETL pipeline orchestrator for single-date processing.
///
/// Implements Section 5 (Functional Processing Flow):
///   The caller supplies an effective date. The pipeline compares the effective
///   date's address snapshot against the previous calendar day's snapshot
///   (effective_date - 1) and produces a single change log file.
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
    private readonly DateOnly _effectiveDate;

    public Pipeline(string inputDir, string outputDir, DateOnly effectiveDate)
    {
        _inputDir = inputDir;
        _outputDir = outputDir;
        _effectiveDate = effectiveDate;
    }

    /// <summary>
    /// Executes the ETL pipeline for the configured effective date.
    ///
    /// Processing flow:
    ///   1. Compute previous date = effective_date - 1 calendar day
    ///   2. Validate that required input files exist (pre-flight)
    ///   3. Load previous-day address snapshot
    ///   4. Load effective-date address snapshot
    ///   5. Resolve and load customer file (<= effective date)
    ///   6. Detect changes (NEW, UPDATED, DELETED)
    ///   7. Write output file
    ///
    /// Section 7.1: Running multiple times with same input produces identical output (idempotent).
    /// </summary>
    public void Run()
    {
        var totalTimer = Stopwatch.StartNew();

        // Compute the comparison date: previous calendar day
        var previousDate = _effectiveDate.AddDays(-1);

        Log("Starting Address Changes ETL");
        Log($"Effective date:   {_effectiveDate:yyyyMMdd}");
        Log($"Previous date:    {previousDate:yyyyMMdd}");
        Log($"Input directory:  {_inputDir}");
        Log($"Output directory: {_outputDir}");

        // --- Pre-flight validation ---
        // Verify both address files exist before loading anything.
        InputValidator.ValidateRequiredFiles(_inputDir, _effectiveDate, previousDate);

        // --- Load previous-day address snapshot ---
        var prevPath = BuildAddressPath(previousDate);
        var prevTimer = Stopwatch.StartNew();
        var previousAddresses = AddressFileReader.ReadFile(prevPath);
        prevTimer.Stop();
        Log($"Loaded previous-day addresses ({previousDate:yyyyMMdd}): {previousAddresses.Count:N0} records ({prevTimer.ElapsedMilliseconds}ms)");

        // --- Load effective-date address snapshot ---
        var currPath = BuildAddressPath(_effectiveDate);
        var currTimer = Stopwatch.StartNew();
        var currentAddresses = AddressFileReader.ReadFile(currPath);
        currTimer.Stop();
        Log($"Loaded effective-date addresses ({_effectiveDate:yyyyMMdd}): {currentAddresses.Count:N0} records ({currTimer.ElapsedMilliseconds}ms)");

        // --- Resolve and load customer file ---
        // Section 2.3, Decision D-2 (Option B): most recent customer file <= effective date
        // Implements BR-13: Customer file does not need to match the effective date exactly.
        var customerDates = DiscoverCustomerDates();
        var resolvedCustomerDate = ResolveCustomerDate(customerDates, _effectiveDate);

        if (resolvedCustomerDate is null)
        {
            // Section 6.2: Halt if no customer file can be resolved
            throw new InvalidOperationException(
                $"No customer file available for effective date {_effectiveDate:yyyyMMdd}. " +
                $"Searched for customers_YYYYMMDD.csv with date <= {_effectiveDate:yyyyMMdd} (Section 6.2)");
        }

        var custPath = Path.Combine(_inputDir, $"customers_{resolvedCustomerDate.Value:yyyyMMdd}.csv");
        if (!File.Exists(custPath))
        {
            throw new InvalidOperationException($"Resolved customer file does not exist: {custPath}");
        }

        var custTimer = Stopwatch.StartNew();
        var customerNames = CustomerFileReader.ReadFile(custPath);
        custTimer.Stop();
        Log($"Loaded customer file {resolvedCustomerDate.Value:yyyyMMdd}: {customerNames.Count:N0} customers ({custTimer.ElapsedMilliseconds}ms)");

        // --- Change detection ---
        // Section 4.1: Compare current-day vs previous-day by address_id
        var detectTimer = Stopwatch.StartNew();
        var changes = ChangeDetector.DetectChanges(previousAddresses, currentAddresses, customerNames);
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
        Log($"Changes detected: {changes.Count:N0} (NEW={newCount:N0}, UPDATED={updatedCount:N0}, DELETED={deletedCount:N0}) ({detectTimer.ElapsedMilliseconds}ms)");

        // --- Write output file ---
        // Implements BR-8: File is always produced, even with zero changes.
        var outputPath = Path.Combine(_outputDir, $"address_changes_{_effectiveDate:yyyyMMdd}.csv");
        var writeTimer = Stopwatch.StartNew();
        ChangeLogWriter.Write(outputPath, changes);
        writeTimer.Stop();
        Log($"Output written: {outputPath} ({writeTimer.ElapsedMilliseconds}ms)");

        totalTimer.Stop();
        Log($"ETL processing complete. Total elapsed: {totalTimer.Elapsed.TotalSeconds:F1}s");
    }

    /// <summary>
    /// Discovers all customer file dates in the input directory.
    /// Used for customer file resolution (Section 2.3).
    /// </summary>
    private List<DateOnly> DiscoverCustomerDates()
    {
        var pattern = "customers_*.csv";
        var files = Directory.GetFiles(_inputDir, pattern);
        var dates = new List<DateOnly>();
        const int prefixLen = 10; // "customers_"

        foreach (var file in files)
        {
            var name = Path.GetFileNameWithoutExtension(file);
            if (name.Length < prefixLen + 8)
                continue;

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
    /// Resolves the customer file to use for the effective date.
    /// Implements Section 2.3, Decision D-2 (Option B):
    ///   Use the most recent customer file whose date is &lt;= the effective date.
    ///
    /// Uses binary search for O(log N) lookup on sorted customer dates.
    /// </summary>
    private static DateOnly? ResolveCustomerDate(List<DateOnly> sortedDates, DateOnly effectiveDate)
    {
        if (sortedDates.Count == 0)
            return null;

        int index = sortedDates.BinarySearch(effectiveDate);

        if (index >= 0)
            return sortedDates[index]; // exact match

        int insertionPoint = ~index;
        if (insertionPoint == 0)
            return null; // all customer dates are after the effective date

        return sortedDates[insertionPoint - 1];
    }

    private string BuildAddressPath(DateOnly date)
    {
        return Path.Combine(_inputDir, $"addresses_{date:yyyyMMdd}.csv");
    }

    private static void Log(string message)
    {
        Console.Error.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] {message}");
    }
}

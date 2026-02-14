namespace ClaudeAddressChanges.Validation;

/// <summary>
/// Pre-flight validation of input data before pipeline execution.
/// Fails fast before any data loading to avoid wasted I/O on large files.
/// </summary>
public static class InputValidator
{
    /// <summary>
    /// Validates that the required address snapshot files exist for the effective date
    /// and its previous calendar day.
    ///
    /// The ETL requires two address files:
    ///   - addresses_YYYYMMDD.csv for the effective date (current-day snapshot)
    ///   - addresses_YYYYMMDD.csv for effective date - 1 (previous-day snapshot)
    ///
    /// Section 6.1, Decision D-5: Halt if a required address file is missing.
    /// </summary>
    public static void ValidateRequiredFiles(string inputDir, DateOnly effectiveDate, DateOnly previousDate)
    {
        var previousPath = Path.Combine(inputDir, $"addresses_{previousDate:yyyyMMdd}.csv");
        if (!File.Exists(previousPath))
        {
            throw new InvalidOperationException(
                $"Previous-day address file not found: {previousPath}. " +
                $"The effective date {effectiveDate:yyyyMMdd} requires the prior day's " +
                $"snapshot ({previousDate:yyyyMMdd}) for comparison.");
        }

        var currentPath = Path.Combine(inputDir, $"addresses_{effectiveDate:yyyyMMdd}.csv");
        if (!File.Exists(currentPath))
        {
            throw new InvalidOperationException(
                $"Effective-date address file not found: {currentPath}.");
        }
    }
}

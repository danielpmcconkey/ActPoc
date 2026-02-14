namespace ClaudeAddressChanges.Validation;

/// <summary>
/// Pre-flight validation of input data before pipeline execution.
/// Implements Section 5.1, Step 5 and Section 6 error conditions.
/// </summary>
public static class InputValidator
{
    /// <summary>
    /// Validates that at least two address dates exist.
    /// Section 5.1, Step 5: The pipeline requires a baseline (first date) plus at least
    /// one processing date to produce output. With only one date, there is nothing to
    /// compare against, so processing completes with no output.
    ///
    /// Implements BR-1: The first date is the baseline; output starts from the second date.
    /// </summary>
    public static void ValidateMinimumDates(List<DateOnly> addressDates)
    {
        if (addressDates.Count == 0)
        {
            throw new InvalidOperationException(
                "No address snapshot files found in the input directory. " +
                "Expected files matching pattern: addresses_YYYYMMDD.csv");
        }

        if (addressDates.Count < 2)
        {
            throw new InvalidOperationException(
                $"Only one address snapshot found ({addressDates[0]:yyyyMMdd}). " +
                "At least two snapshots are required: one baseline and one processing date. " +
                "(Section 5.1, Step 5)");
        }
    }
}

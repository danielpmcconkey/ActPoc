using System.Text;

namespace ClaudeAddressChanges.IO;

/// <summary>
/// Reads a customers_YYYYMMDD.csv file and builds a lookup of customer_id to derived customer_name.
///
/// Implements BR-6: customer_name = first_name + " " + last_name
/// Implements BR-7: Only first_name and last_name are used; prefix, suffix, sort_name,
///                  and birthdate are explicitly excluded from output.
/// </summary>
public static class CustomerFileReader
{
    private const int ExpectedFieldCount = 7;

    private static readonly string[] ExpectedHeaders =
    {
        "id", "prefix", "first_name", "last_name", "sort_name", "suffix", "birthdate"
    };

    /// <summary>
    /// Reads and validates a customer snapshot file.
    /// Returns a Dictionary mapping customer id to the pre-computed customer_name string.
    ///
    /// Only fields used: id (index 0), first_name (index 2), last_name (index 3).
    /// All other fields are read and discarded per BR-7.
    ///
    /// Pre-computing the concatenated name avoids repeated string allocation during
    /// change detection (optimized for joins against up to 10M address records).
    /// </summary>
    public static Dictionary<int, string> ReadFile(string path)
    {
        var customerNames = new Dictionary<int, string>(500_000);

        using var reader = new StreamReader(path, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536);

        // Read and validate header
        var headerLine = reader.ReadLine()
            ?? throw new InvalidOperationException($"Customer file is empty: {path}");
        ValidateHeader(headerLine, path);

        int lineNumber = 1;
        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            lineNumber++;

            if (string.IsNullOrWhiteSpace(line))
                continue;

            var fields = CsvFieldParser.Parse(line);

            // Section 6.5: Validate field count
            if (fields.Length != ExpectedFieldCount)
            {
                throw new InvalidOperationException(
                    $"Customer file {path} line {lineNumber}: expected {ExpectedFieldCount} fields, got {fields.Length}");
            }

            if (!int.TryParse(fields[0], out int id))
            {
                throw new InvalidOperationException(
                    $"Customer file {path} line {lineNumber}: invalid id '{fields[0]}'");
            }

            // Section 2.2: first_name and last_name are not nullable
            string firstName = fields[2] ?? throw new InvalidOperationException(
                $"Customer file {path} line {lineNumber}: first_name cannot be NULL for customer {id}");

            string lastName = fields[3] ?? throw new InvalidOperationException(
                $"Customer file {path} line {lineNumber}: last_name cannot be NULL for customer {id}");

            // Implements BR-6: customer_name = first_name + " " + last_name
            // Unicode characters, accents, hyphens, and apostrophes are preserved as-is
            // (Section 4.3 requirement).
            string customerName = $"{firstName} {lastName}";

            if (!customerNames.TryAdd(id, customerName))
            {
                throw new InvalidOperationException(
                    $"Customer file {path} line {lineNumber}: duplicate customer id {id}");
            }
        }

        return customerNames;
    }

    private static void ValidateHeader(string headerLine, string path)
    {
        var headers = CsvFieldParser.Parse(headerLine);

        if (headers.Length != ExpectedFieldCount)
        {
            throw new InvalidOperationException(
                $"Customer file {path}: header has {headers.Length} columns, expected {ExpectedFieldCount}");
        }

        for (int i = 0; i < ExpectedFieldCount; i++)
        {
            if (!string.Equals(headers[i], ExpectedHeaders[i], StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Customer file {path}: header column {i} is '{headers[i]}', expected '{ExpectedHeaders[i]}'");
            }
        }
    }
}

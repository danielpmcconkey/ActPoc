using System.Text;
using ClaudeAddressChanges.Models;

namespace ClaudeAddressChanges.IO;

/// <summary>
/// Reads an addresses_YYYYMMDD.csv file into a dictionary keyed by address_id.
/// Implements Section 2.1 (input schema) and Section 6.4 (duplicate address_id validation).
/// </summary>
public static class AddressFileReader
{
    private const int ExpectedFieldCount = 9;

    private static readonly string[] ExpectedHeaders =
    {
        "address_id", "customer_id", "address_line1", "city",
        "state_province", "postal_code", "country", "start_date", "end_date"
    };

    /// <summary>
    /// Reads and validates an address snapshot file.
    /// Returns a Dictionary keyed by address_id for O(1) lookup during change detection.
    ///
    /// Validates:
    ///   - Header matches expected schema (Section 6.5)
    ///   - Each row has exactly 9 fields (Section 6.5)
    ///   - address_id and customer_id are valid integers
    ///   - No duplicate address_id values (Section 6.4, Decision D-4)
    ///
    /// Uses a 64KB read buffer for efficient I/O with large files (up to 10M records).
    /// </summary>
    public static Dictionary<int, AddressRecord> ReadFile(string path)
    {
        // Pre-allocate for expected scale. Dictionary will resize if needed.
        var addresses = new Dictionary<int, AddressRecord>(1_000_000);

        using var reader = new StreamReader(path, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536);

        // Read and validate header
        var headerLine = reader.ReadLine()
            ?? throw new InvalidOperationException($"Address file is empty: {path}");
        ValidateHeader(headerLine, path);

        int lineNumber = 1;
        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            lineNumber++;

            // Skip empty lines
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var fields = CsvFieldParser.Parse(line);

            // Section 6.5: Validate field count
            if (fields.Length != ExpectedFieldCount)
            {
                throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: expected {ExpectedFieldCount} fields, got {fields.Length}");
            }

            // Parse required integer fields
            if (!int.TryParse(fields[0], out int addressId))
            {
                throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: invalid address_id '{fields[0]}'");
            }

            if (!int.TryParse(fields[1], out int customerId))
            {
                throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: invalid customer_id '{fields[1]}'");
            }

            var record = new AddressRecord(
                AddressId: addressId,
                CustomerId: customerId,
                AddressLine1: fields[2] ?? throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: address_line1 cannot be NULL"),
                City: fields[3] ?? throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: city cannot be NULL"),
                StateProvince: fields[4] ?? throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: state_province cannot be NULL"),
                PostalCode: fields[5] ?? throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: postal_code cannot be NULL"),
                Country: fields[6] ?? throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: country cannot be NULL"),
                StartDate: fields[7] ?? throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: start_date cannot be NULL"),
                EndDate: fields[8]  // Nullable per Section 2.1
            );

            // Section 6.4, Decision D-4: Halt on duplicate address_id
            if (!addresses.TryAdd(addressId, record))
            {
                throw new InvalidOperationException(
                    $"Address file {path} line {lineNumber}: duplicate address_id {addressId} " +
                    "(Section 6.4: duplicate primary keys indicate corrupt input)");
            }
        }

        return addresses;
    }

    private static void ValidateHeader(string headerLine, string path)
    {
        var headers = CsvFieldParser.Parse(headerLine);

        if (headers.Length != ExpectedFieldCount)
        {
            throw new InvalidOperationException(
                $"Address file {path}: header has {headers.Length} columns, expected {ExpectedFieldCount}");
        }

        for (int i = 0; i < ExpectedFieldCount; i++)
        {
            if (!string.Equals(headers[i], ExpectedHeaders[i], StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Address file {path}: header column {i} is '{headers[i]}', expected '{ExpectedHeaders[i]}'");
            }
        }
    }
}

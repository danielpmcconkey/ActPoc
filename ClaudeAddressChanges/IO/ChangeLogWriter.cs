using System.Text;
using ClaudeAddressChanges.Models;

namespace ClaudeAddressChanges.IO;

/// <summary>
/// Writes the output address_changes_YYYYMMDD.csv file.
/// Implements Section 3.1 (output schema), Section 3.2 (file structure),
/// Section 3.3 (quoting rules), and Section 7.3 (atomic write).
/// </summary>
public static class ChangeLogWriter
{
    // Section 3.2: Header row — field names, comma-separated, unquoted
    private const string Header =
        "change_type,address_id,customer_id,customer_name,address_line1," +
        "city,state_province,postal_code,country,start_date,end_date";

    /// <summary>
    /// Writes the change log to the specified path.
    ///
    /// File structure (Section 3.2):
    ///   Line 1:       Header row
    ///   Lines 2..N+1: Data rows (zero or more)
    ///   Line N+2:     Empty/blank line
    ///   Line N+3:     Footer: "Expected records: N"
    ///
    /// Implements Section 7.3: Atomic write via temp file + rename to prevent
    /// downstream consumers from reading partial files.
    /// </summary>
    public static void Write(string outputPath, IReadOnlyList<AddressChange> changes)
    {
        var tempPath = outputPath + ".tmp";

        try
        {
            // UTF-8 without BOM; 64KB buffer for efficient I/O
            using (var writer = new StreamWriter(tempPath, append: false,
                new UTF8Encoding(encoderShouldEmitUTF8Identifier: false), bufferSize: 65536))
            {
                // Section 3.2: Header row
                writer.WriteLine(Header);

                // Section 3.2: Data rows
                foreach (var change in changes)
                {
                    WriteDataRow(writer, change);
                }

                // Section 3.2: Blank line before footer
                writer.WriteLine();

                // Implements BR-9: Record count footer
                // Section 4.7: "Expected records: N" where N = data row count
                writer.WriteLine($"Expected records: {changes.Count}");
            }

            // Section 7.3: Atomic rename
            File.Move(tempPath, outputPath, overwrite: true);
        }
        catch
        {
            // Clean up temp file on failure
            try { File.Delete(tempPath); } catch { /* best effort */ }
            throw;
        }
    }

    /// <summary>
    /// Writes a single data row with field-level quoting per Section 3.3.
    ///
    /// Quoting rules (Section 3.3):
    ///   Quoted:   customer_name, address_line1, city, state_province, postal_code
    ///   Unquoted: change_type, address_id, customer_id, country, start_date, end_date
    ///
    /// Implements BR-10: NULL end_date rendered as empty (blank field).
    /// </summary>
    private static void WriteDataRow(StreamWriter writer, AddressChange change)
    {
        // Field 1: change_type — unquoted (Section 3.3)
        writer.Write(change.ChangeType.ToString());
        writer.Write(',');

        // Field 2: address_id — unquoted integer (Section 3.3)
        writer.Write(change.AddressId);
        writer.Write(',');

        // Field 3: customer_id — unquoted integer (Section 3.3)
        writer.Write(change.CustomerId);
        writer.Write(',');

        // Field 4: customer_name — quoted (Section 3.3)
        WriteQuotedField(writer, change.CustomerName);
        writer.Write(',');

        // Field 5: address_line1 — quoted (Section 3.3)
        WriteQuotedField(writer, change.AddressLine1);
        writer.Write(',');

        // Field 6: city — quoted (Section 3.3)
        WriteQuotedField(writer, change.City);
        writer.Write(',');

        // Field 7: state_province — quoted (Section 3.3)
        WriteQuotedField(writer, change.StateProvince);
        writer.Write(',');

        // Field 8: postal_code — quoted (Section 3.3)
        WriteQuotedField(writer, change.PostalCode);
        writer.Write(',');

        // Field 9: country — unquoted (Section 3.3)
        writer.Write(change.Country);
        writer.Write(',');

        // Field 10: start_date — unquoted (Section 3.3)
        writer.Write(change.StartDate);
        writer.Write(',');

        // Field 11: end_date — unquoted (Section 3.3)
        // Implements BR-10, Section 4.5: NULL -> empty (blank field)
        writer.Write(change.EndDate ?? "");

        writer.WriteLine();
    }

    /// <summary>
    /// Writes a field enclosed in double quotes.
    /// If the value contains double quotes, they are escaped by doubling (standard CSV).
    /// </summary>
    private static void WriteQuotedField(StreamWriter writer, string value)
    {
        writer.Write('"');
        if (value.Contains('"'))
            writer.Write(value.Replace("\"", "\"\""));
        else
            writer.Write(value);
        writer.Write('"');
    }
}

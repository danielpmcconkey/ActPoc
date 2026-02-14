namespace ClaudeAddressChanges.IO;

/// <summary>
/// Low-level CSV line parser.
/// Handles quoted fields (with escaped double-quotes) and the NULL literal convention
/// defined in Section 2.1: unquoted NULL is returned as null.
/// </summary>
public static class CsvFieldParser
{
    /// <summary>
    /// Parses a single CSV line into an array of nullable field values.
    ///
    /// Rules:
    ///   - Quoted fields: enclosing quotes stripped, internal "" unescaped to ".
    ///   - Unquoted fields: returned as-is, except the literal NULL which becomes null.
    ///   - Trailing comma produces an empty trailing field.
    ///   - Designed for high-throughput: avoids StringBuilder in the common case
    ///     (quoted fields without escaped quotes).
    /// </summary>
    public static string?[] Parse(string line)
    {
        var fields = new List<string?>(12);
        int pos = 0;
        int len = line.Length;

        while (true)
        {
            if (pos >= len)
            {
                // Reached end of line after a comma separator — trailing empty field.
                // Also handles the degenerate case of a completely empty line.
                if (fields.Count > 0 || len == 0)
                    fields.Add("");
                break;
            }

            if (line[pos] == '"')
            {
                pos++; // skip opening quote
                int start = pos;
                bool hasEscapedQuotes = false;

                while (pos < len)
                {
                    if (line[pos] == '"')
                    {
                        if (pos + 1 < len && line[pos + 1] == '"')
                        {
                            hasEscapedQuotes = true;
                            pos += 2;
                        }
                        else
                        {
                            break; // closing quote
                        }
                    }
                    else
                    {
                        pos++;
                    }
                }

                // Extract field value, unescape only when necessary (optimization)
                string value = line[start..pos];
                if (hasEscapedQuotes)
                    value = value.Replace("\"\"", "\"");

                fields.Add(value);

                if (pos < len) pos++; // skip closing quote
                if (pos < len && line[pos] == ',')
                    pos++; // skip comma separator
                else if (pos >= len)
                    break; // end of line after closing quote
            }
            else
            {
                // Unquoted field
                int start = pos;
                while (pos < len && line[pos] != ',')
                    pos++;

                string raw = line[start..pos];

                // Section 2.1: NULL is represented as the literal unquoted string NULL
                fields.Add(raw == "NULL" ? null : raw);

                if (pos < len)
                    pos++; // skip comma, continue to next field
                else
                    break; // end of line
            }
        }

        return fields.ToArray();
    }
}

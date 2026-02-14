namespace ClaudeAddressChanges.Models;

/// <summary>
/// Change classification for address records.
/// Implements Section 4.2: Change Type Assignment.
/// </summary>
public enum ChangeType
{
    /// <summary>address_id present in current day, absent in previous day (BR-3)</summary>
    NEW,

    /// <summary>address_id present in both days, at least one field differs (BR-4)</summary>
    UPDATED,

    /// <summary>address_id absent in current day, present in previous day (Decision D-1, A-1 Option A)</summary>
    DELETED
}

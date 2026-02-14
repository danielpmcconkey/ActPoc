using ClaudeAddressChanges.Models;

namespace ClaudeAddressChanges.Engine;

/// <summary>
/// Core change detection engine.
/// Implements Section 4.1 (change detection), Section 4.2 (change type assignment),
/// Section 4.3 (customer name enrichment), and Section 4.6 (output ordering).
///
/// Algorithm complexity for N addresses:
///   Time:  O(N) for comparison + O(K log K) for sorting K changes
///   Space: O(K) for the change list (both input dictionaries are caller-owned)
/// </summary>
public static class ChangeDetector
{
    /// <summary>
    /// Compares current-day and previous-day address snapshots to produce a sorted list
    /// of address changes.
    ///
    /// Processing logic (Section 5.2, Steps 12-15):
    ///   1. For each current-day record: classify as NEW or UPDATED (or skip if unchanged)
    ///   2. For each previous-day record not in current day: classify as DELETED
    ///   3. Sort all changes by address_id ascending
    /// </summary>
    /// <param name="previousDay">Previous day's addresses keyed by address_id</param>
    /// <param name="currentDay">Current day's addresses keyed by address_id</param>
    /// <param name="customerNames">Customer id to name lookup (pre-computed first + " " + last)</param>
    /// <returns>Sorted list of address changes for the output file</returns>
    public static List<AddressChange> DetectChanges(
        Dictionary<int, AddressRecord> previousDay,
        Dictionary<int, AddressRecord> currentDay,
        Dictionary<int, string> customerNames)
    {
        var changes = new List<AddressChange>();

        // --- Pass 1: Scan current-day records for NEW and UPDATED ---

        foreach (var (addressId, current) in currentDay)
        {
            if (previousDay.TryGetValue(addressId, out var previous))
            {
                // Implements BR-4: address_id exists in both days.
                // Record equality uses the auto-generated member-wise comparison
                // from the C# record type, which compares ALL fields including
                // address_id, customer_id, all address fields, and end_date.
                // Section 4.1: NULL-to-NULL is equal (C# null == null is true).
                if (!current.Equals(previous))
                {
                    // Implements BR-4: At least one field differs -> UPDATED
                    // Implements BR-5: Output uses current-day values (post-change state)
                    changes.Add(CreateChange(ChangeType.UPDATED, current, customerNames));
                }
                // Implements BR-11: All fields identical -> excluded from output
            }
            else
            {
                // Implements BR-3: address_id not in previous day -> NEW
                // Implements BR-5: Output uses current-day values
                changes.Add(CreateChange(ChangeType.NEW, current, customerNames));
            }
        }

        // --- Pass 2: Scan previous-day records for DELETED ---
        // Implements Decision D-1 (A-1 Option A): Emit DELETED records for complete audit trail.
        // Section 4.1: address_id in previous day but absent from current day -> DELETED.

        foreach (var (addressId, previous) in previousDay)
        {
            if (!currentDay.ContainsKey(addressId))
            {
                // Implements A-1 Option A: DELETED using previous-day field values
                // Section 4.3: For DELETED records, customer_id from previous-day record
                // is used for the name lookup.
                changes.Add(CreateChange(ChangeType.DELETED, previous, customerNames));
            }
        }

        // Implements BR-12, Section 4.6: Sort by address_id ascending
        changes.Sort((a, b) => a.AddressId.CompareTo(b.AddressId));

        return changes;
    }

    /// <summary>
    /// Creates an AddressChange from an address record with customer name enrichment.
    /// Implements BR-6, Section 4.3: Customer name lookup via customer_id.
    /// Implements Section 4.3, Decision D-3: Halt on orphan customer_id.
    /// </summary>
    private static AddressChange CreateChange(
        ChangeType changeType,
        AddressRecord address,
        Dictionary<int, string> customerNames)
    {
        // Section 4.3, Decision D-3 (A-5 Option C): Halt with error if no matching customer
        if (!customerNames.TryGetValue(address.CustomerId, out var customerName))
        {
            throw new InvalidOperationException(
                $"Orphan customer_id {address.CustomerId} for address_id {address.AddressId}: " +
                $"no matching customer record found (Section 4.3, Decision D-3)");
        }

        return new AddressChange(
            ChangeType: changeType,
            AddressId: address.AddressId,
            CustomerId: address.CustomerId,
            CustomerName: customerName,
            AddressLine1: address.AddressLine1,
            City: address.City,
            StateProvince: address.StateProvince,
            PostalCode: address.PostalCode,
            Country: address.Country,
            StartDate: address.StartDate,
            EndDate: address.EndDate);
    }
}

namespace ClaudeAddressChanges.Models;

/// <summary>
/// Represents a single address from a daily snapshot (Section 2.1).
///
/// Uses C# record type for automatic member-wise equality comparison, which implements:
///   BR-4:  Any field difference between days = UPDATED
///   BR-11: All fields identical between days = unchanged (excluded from output)
///
/// Section 4.1 constraint: NULL-to-NULL is considered equal.
/// C# record equality treats null == null as true, satisfying this requirement.
///
/// Dates are stored as strings because change detection is value-comparison only;
/// no date arithmetic is performed on address records.
/// </summary>
public sealed record AddressRecord(
    int AddressId,
    int CustomerId,
    string AddressLine1,
    string City,
    string StateProvince,
    string PostalCode,
    string Country,
    string StartDate,
    string? EndDate);

namespace ClaudeAddressChanges.Models;

/// <summary>
/// Represents a single row in the output change log (Section 3.1).
/// Field order matches the output schema defined in Section 3.1.
/// </summary>
public sealed record AddressChange(
    ChangeType ChangeType,
    int AddressId,
    int CustomerId,
    string CustomerName,
    string AddressLine1,
    string City,
    string StateProvince,
    string PostalCode,
    string Country,
    string StartDate,
    string? EndDate);

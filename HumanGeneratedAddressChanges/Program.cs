using System.Reflection.Metadata.Ecma335;
using SharedFunctions;

namespace HumanGeneratedAddressChanges;

class Program
{
    static void Main(string[] args)
    {
        try
        {
            /*
             * Business requirements:
             *      Take a logical date parameter from command line arguments
             *      Pull customer and address data from the logical date and the date prior
             *      If there is no file at logical date use the most recent date (that isn't greater)
             *      same for the date prior
             *      Compare the data. You want to capture changes only to the output file
             *          Output format
             *              change_type : one of "NEW", "REMOVED", "UPDATED"
             *              address_id : integer
             *              customer_id : integer
             *              customer_name : concatenation of the first and last name with a space in between
             *              address_line1 : string
             *              city : string
             *              state_province : string
             *              postal_code : string
             *              country : string
             *              start_date : YYYY-MM-DD
             *              end_date YYYY-MM-DD
             *          A record is considered new if its address_id is not present in the prior date's address table
             *          A record is considered removed if its address_id is present in the prior date's address table but not in the current date's address table
             *          A record is considered updated if the address_id is in both current and prior address tables, but any of the output fields is different 
             */
            var logicalDate = Arguments.ParseLogicalDate(args);
            var priorLogicalDate = logicalDate.AddDays(-1);

            const string dataLakePath = "/media/dan/fdrive/codeprojects/AtcPoc/AtcPoc/DataLake/";
            const string curatedPath = "/media/dan/fdrive/codeprojects/AtcPoc/AtcPoc/Curated/";
            
            // source data at the lake for T and T-minus-1 day
            var addressFileAtT = FileSeek.FindMostRecentFile(dataLakePath, "addresses", logicalDate); 
            var addressFileAtTMinus1 = FileSeek.FindMostRecentFile(dataLakePath, "addresses", priorLogicalDate); 
            var customersFileAtT = FileSeek.FindMostRecentFile(dataLakePath, "customers", logicalDate); 
            var customersFileAtTMinus1 = FileSeek.FindMostRecentFile(dataLakePath, "customers", priorLogicalDate); 
            
            var addressesT = CsvParse.ParseAddresses(addressFileAtT);
            var addressesTMinus1 = CsvParse.ParseAddresses(addressFileAtTMinus1);
            var customersT = CsvParse.ParseCustomers(customersFileAtT);
            var customersTMinus1 = CsvParse.ParseCustomers(customersFileAtTMinus1);
            
            /*
             * note, code below was originally written by Claude using teh following prompt:
             * 
             *      given the business requirements listed in the beginning of the Main method, and given the 4 lists
             *      between lines 47 and 50, add code after line 50 that accomplishes the business requirement
             *
             * But it used foreach loops and I wanted it to be set based. So I added another prompt:
             *
             *      can you do it using linq and set-based operations?
             *
             * I then manually edited it to fit my sensibilities
             * 
             */
            
            
            // Create customer lookup dictionaries
            var customerLookupT = customersT.ToDictionary(c => c.id, c => $"{c.first_name} {c.last_name}");
            var customerLookupTMinus1 = customersTMinus1.ToDictionary(c => c.id, c => $"{c.first_name} {c.last_name}");
            
            // Get address ID sets for set operations
            var addressIdsT = addressesT.Select(a => a.address_id).ToHashSet();
            var addressIdsTMinus1 = addressesTMinus1.Select(a => a.address_id).ToHashSet();
            
            // Create address lookups
            var addressLookupT = addressesT.ToDictionary(a => a.address_id);
            var addressLookupTMinus1 = addressesTMinus1.ToDictionary(a => a.address_id);
            
            // NEW addresses: in T but not in T-1
            var newChanges = addressIdsT
                .Except(addressIdsTMinus1)
                .Select(id => CreateChangeRecord("NEW", addressLookupT[id], customerLookupT));
            
            // REMOVED addresses: in T-1 but not in T
            var removedChanges = addressIdsTMinus1
                .Except(addressIdsT)
                .Select(id => CreateChangeRecord("REMOVED", addressLookupTMinus1[id], customerLookupTMinus1));
            
            // UPDATED addresses: in both T and T-1 but with differences
            var updatedChanges = addressIdsT
                .Intersect(addressIdsTMinus1)
                .Where(id => AreAddressesDifferent(
                    addressLookupT[id], 
                    addressLookupTMinus1[id], 
                    customerLookupT, 
                    customerLookupTMinus1))
                .Select(id => CreateChangeRecord("UPDATED", addressLookupT[id], customerLookupT));
            
            // Combine all changes
            var changes = newChanges
                .Concat(removedChanges)
                .Concat(updatedChanges)
                .OrderBy(c => c.address_id)
                .ToList();
            
            // Write output to curated path
            var outputFileName = Path.Combine(curatedPath, $"address_changes_{logicalDate:yyyyMMdd}.csv");
            WriteChangesToCsv(changes, outputFileName);
            
            Console.WriteLine($"Processing changes for {logicalDate}");
            Console.WriteLine($"Found {changes.Count} changes:");
            Console.WriteLine($"- NEW: {changes.Count(c => c.change_type == "NEW")}");
            Console.WriteLine($"- REMOVED: {changes.Count(c => c.change_type == "REMOVED")}");
            Console.WriteLine($"- UPDATED: {changes.Count(c => c.change_type == "UPDATED")}");
            Console.WriteLine($"Output written to: {outputFileName}");


            
            Console.WriteLine($"Processing changes for {logicalDate}");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        
    }
    
    /// <summary>
    /// this method was written by Claude
    /// </summary>
    private static CustomerAddressChange CreateChangeRecord(
        string changeType, 
        Address address, 
        Dictionary<int, string> customerLookup)
    {
        var customerName = customerLookup.GetValueOrDefault(address.customer_id, "Unknown Customer");
        
        return new CustomerAddressChange(
            changeType,
            address.address_id,
            address.customer_id,
            customerName,
            address.address_line1,
            address.city,
            address.state_province,
            address.postal_code,
            address.country,
            address.start_date,
            address.end_date
        );
    }
    
    /// <summary>
    /// this method was written by Claude
    /// </summary>
    private static bool AreAddressesDifferent(
        Address addressT, 
        Address addressTMinus1, 
        Dictionary<int, string> customerLookupT,
        Dictionary<int, string> customerLookupTMinus1)
    {
        var customerNameT = customerLookupT.GetValueOrDefault(addressT.customer_id, "Unknown Customer");
        var customerNameTMinus1 = customerLookupTMinus1.GetValueOrDefault(addressTMinus1.customer_id, "Unknown Customer");
        
        return addressT.customer_id != addressTMinus1.customer_id ||
               customerNameT != customerNameTMinus1 ||
               addressT.address_line1 != addressTMinus1.address_line1 ||
               addressT.city != addressTMinus1.city ||
               addressT.state_province != addressTMinus1.state_province ||
               addressT.postal_code != addressTMinus1.postal_code ||
               addressT.country != addressTMinus1.country ||
               addressT.start_date != addressTMinus1.start_date ||
               addressT.end_date != addressTMinus1.end_date;
    }
    
    /// <summary>
    /// this method was written by Claude
    /// </summary>
    private static void WriteChangesToCsv(List<CustomerAddressChange> changes, string outputPath)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath) ?? throw new InvalidOperationException());
        
        using var writer = new StreamWriter(outputPath);
        
        // Write header
        writer.WriteLine("change_type,address_id,customer_id,customer_name,address_line1,city,state_province,postal_code,country,start_date,end_date");
        
        // Write data rows
        foreach (var change in changes)
        {
            var endDateStr = change.end_date?.ToString("yyyy-MM-dd") ?? "";
            writer.WriteLine($"{change.change_type},{change.address_id},{change.customer_id},\"{change.customer_name}\",\"{change.address_line1}\",\"{change.city}\",\"{change.state_province}\",\"{change.postal_code}\",{change.country},{change.start_date:yyyy-MM-dd},{endDateStr}");
        }
    }

    
}
using System.Security.Cryptography;
using Npgsql;

namespace ClaudeCoveredTransactions;

class Program
{
    static void Main(string[] args)
    {
        // var accountsOct1 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 1));
        // var accountsOct2 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 2));
        // var accountsOct3 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 3));
        // var accountsOct4 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 4));
        // var accountsOct5 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 5));
        // var accountsOct6 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 6));
        // var accountsOct7 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 7));
        //
        // var addressesOct1 = DataLakeReader.GetAddressesByAsOf(new DateTime(2024, 10, 1));
        // var addressesOct2 = DataLakeReader.GetAddressesByAsOf(new DateTime(2024, 10, 2));
        // var addressesOct3 = DataLakeReader.GetAddressesByAsOf(new DateTime(2024, 10, 3));
        // var addressesOct4 = DataLakeReader.GetAddressesByAsOf(new DateTime(2024, 10, 4));
        // var addressesOct5 = DataLakeReader.GetAddressesByAsOf(new DateTime(2024, 10, 5));
        // var addressesOct6 = DataLakeReader.GetAddressesByAsOf(new DateTime(2024, 10, 6));
        // var addressesOct7 = DataLakeReader.GetAddressesByAsOf(new DateTime(2024, 10, 7));
        
        // var customersOct1 = DataLakeReader.GetCustomersByAsOf(new DateTime(2024, 10, 1));
        // var customersOct2 = DataLakeReader.GetCustomersByAsOf(new DateTime(2024, 10, 2));
        // var customersOct3 = DataLakeReader.GetCustomersByAsOf(new DateTime(2024, 10, 3));
        // var customersOct4 = DataLakeReader.GetCustomersByAsOf(new DateTime(2024, 10, 4));
        // var customersOct5 = DataLakeReader.GetCustomersByAsOf(new DateTime(2024, 10, 5));
        // var customersOct6 = DataLakeReader.GetCustomersByAsOf(new DateTime(2024, 10, 6));
        // var customersOct7 = DataLakeReader.GetCustomersByAsOf(new DateTime(2024, 10, 7));
        
        // var customerSegmentsOct1 = DataLakeReader.GetCustomerSegmentsByAsOf(new DateTime(2024, 10, 1));
        // var customerSegmentsOct2 = DataLakeReader.GetCustomerSegmentsByAsOf(new DateTime(2024, 10, 2));
        // var customerSegmentsOct3 = DataLakeReader.GetCustomerSegmentsByAsOf(new DateTime(2024, 10, 3));
        // var customerSegmentsOct4 = DataLakeReader.GetCustomerSegmentsByAsOf(new DateTime(2024, 10, 4));
        // var customerSegmentsOct5 = DataLakeReader.GetCustomerSegmentsByAsOf(new DateTime(2024, 10, 5));
        // var customerSegmentsOct6 = DataLakeReader.GetCustomerSegmentsByAsOf(new DateTime(2024, 10, 6));
        // var customerSegmentsOct7 = DataLakeReader.GetCustomerSegmentsByAsOf(new DateTime(2024, 10, 7));
        //
        // var segmentsOct1 = DataLakeReader.GetSegmentsByAsOf(new DateTime(2024, 10, 1));
        // var segmentsOct2 = DataLakeReader.GetSegmentsByAsOf(new DateTime(2024, 10, 2));
        // var segmentsOct3 = DataLakeReader.GetSegmentsByAsOf(new DateTime(2024, 10, 3));
        // var segmentsOct4 = DataLakeReader.GetSegmentsByAsOf(new DateTime(2024, 10, 4));
        // var segmentsOct5 = DataLakeReader.GetSegmentsByAsOf(new DateTime(2024, 10, 5));
        // var segmentsOct6 = DataLakeReader.GetSegmentsByAsOf(new DateTime(2024, 10, 6));
        // var segmentsOct7 = DataLakeReader.GetSegmentsByAsOf(new DateTime(2024, 10, 7));
        //
        // var transactionsOct1 = DataLakeReader.GetTransactionsByAsOf(new DateTime(2024, 10, 1));
        // var transactionsOct2 = DataLakeReader.GetTransactionsByAsOf(new DateTime(2024, 10, 2));
        // var transactionsOct3 = DataLakeReader.GetTransactionsByAsOf(new DateTime(2024, 10, 3));
        // var transactionsOct4 = DataLakeReader.GetTransactionsByAsOf(new DateTime(2024, 10, 4));
        // var transactionsOct5 = DataLakeReader.GetTransactionsByAsOf(new DateTime(2024, 10, 5));
        // var transactionsOct6 = DataLakeReader.GetTransactionsByAsOf(new DateTime(2024, 10, 6));
        // var transactionsOct7 = DataLakeReader.GetTransactionsByAsOf(new DateTime(2024, 10, 7));
    }
}
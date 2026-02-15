using System.Security.Cryptography;
using Npgsql;

namespace ClaudeCoveredTransactions;

class Program
{
    static void Main(string[] args)
    {
        var accountsOct1 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 1));
        var accountsOct2 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 2));
        var accountsOct3 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 3));
        var accountsOct4 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 4));
        var accountsOct5 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 5));
        var accountsOct6 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 6));
        var accountsOct7 = DataLakeReader.GetAccountsByAsOf(new DateTime(2024, 10, 7));
    }
}
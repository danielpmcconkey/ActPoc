using System.Globalization;
using System.Text.RegularExpressions;

namespace SharedFunctions;

public static class FileSeek
{
    public static string FindMostRecentFile(string dataLakePath, string namePrefix, DateTime logicalDate)
    {
        if (!Directory.Exists(dataLakePath))
        {
            throw new DirectoryNotFoundException($"Directory not found: {dataLakePath}");
        }

        var pattern = "^" + namePrefix + @"_(\d{8})\.csv$";
        var regex = new Regex(pattern, RegexOptions.IgnoreCase);
        
        var candidateFiles = new List<(DateTime date, string fullPath)>();

        foreach (var file in Directory.GetFiles(dataLakePath, $"{namePrefix}*.csv"))
        {
            var fileName = Path.GetFileName(file);
            var match = regex.Match(fileName);
            
            if (match.Success)
            {
                var dateString = match.Groups[1].Value;
                if (DateTime.TryParseExact(dateString, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime fileDate))
                {
                    if (fileDate <= logicalDate)
                    {
                        candidateFiles.Add((fileDate, file));
                    }
                }
            }
        }

        if (candidateFiles.Count == 0)
        {
            throw new FileNotFoundException($"No {namePrefix} file found with date on or before {logicalDate:yyyy-MM-dd}");
        }

        // Find the most recent file that's not greater than logicalDate
        var mostRecentFile = candidateFiles.OrderByDescending(x => x.date).First();
        
        return mostRecentFile.fullPath;
    }

    
}
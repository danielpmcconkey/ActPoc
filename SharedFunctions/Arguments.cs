namespace SharedFunctions;

public static class Arguments
{
    public static DateTime ParseLogicalDate(string[] args)
    {
        if (args.Length == 0)
        {
            throw new ArgumentException("No arguments provided");
        }

        foreach (var arg in args)
        {
            if (!arg.StartsWith("--logicalDate=")) continue;
                
            var dateString = arg.Substring("--logicalDate=".Length);
            if (!DateTime.TryParse(dateString, out var parsedDate))
                throw new ArgumentException($"Invalid date format for --logicalDate: {dateString}");
            return parsedDate;
        }
        throw new ArgumentException($"Missing --logicalDate argument");
    }
}
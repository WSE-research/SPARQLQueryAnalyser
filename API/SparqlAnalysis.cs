namespace API;

public enum DbType
{
    Unknown = -1, Stardog
}

public class PlainTextAnalysisConfig
{
    public IEnumerable<string> Queries { get; } = new List<string>();
}

public class DatabaseConfig
{
    public DbType Type { get; }
    public string SelectQuery { get; }
    public string Username { get; }
    public string Password { get; }
    public string DatabaseName { get; }
    public string DatabaseUri { get; }
    public int DatabasePort { get; }
    public bool Upload { get; }

    public DatabaseConfig(DbType type, string databaseUri, int databasePort, string databaseName, string username, 
        string password, string selectQuery, bool upload)
    {
        Type = type;
        DatabaseUri = databaseUri;
        DatabasePort = databasePort;
        DatabaseName = databaseName;
        Username = username;
        Password = password;
        SelectQuery = selectQuery;
        Upload = upload;
    }
}

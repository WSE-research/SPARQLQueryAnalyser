using System.ComponentModel;
using System.Globalization;
using System.Text.RegularExpressions;
using VDS.RDF.Query;
using VDS.RDF.Query.Patterns;
using VDS.RDF.Storage;

namespace SPARQLParser;

/// <summary>
/// Interface for fetching SPARQL queries from a database and uploading generated statistics
/// </summary>
public interface IDbConnector
{
    /// <summary>
    /// Use current DB to upload statistics
    /// </summary>
    /// <returns></returns>
    public bool UploadToDb();
    
    /// <summary>
    /// Execute an INSERT query for the statistics generated for the managed SPARQL queries
    /// </summary>
    /// <param name="query">INSERT query for stardog database</param>
    public void UploadStats(string query);
    
    /// <summary>
    /// Load all queries from DB and build a list with them.
    /// </summary>
    /// <returns>List of strings in the format "SPARQLQuery/||\QuestionID"</returns>
    public IEnumerable<string> GetQueries();
}

/// <summary>
/// Class providing SPARQL queries stored with Stardog
/// </summary>
public class StardogSparqlConnector: IDbConnector
{
    private readonly StardogConnector _connector;
    private readonly string _selectQueriesQuery;
    private readonly string _questionId;
    private readonly string _queryId;
    private readonly string _queryText;
    private readonly bool _upload;

    /// <summary>
    /// Initialize connection to a Stardog instance
    /// </summary>
    /// <param name="stardogBaseUri">Base URI for the used Stardog instance (e. g. http://localhost:5820)</param>
    /// <param name="stardogUsername">Stardog username</param>
    /// <param name="stardogPassword">Stardog password</param>
    /// <param name="database">Database used as query storage</param>
    /// <param name="selectQueriesQuery">Query used to get the stored SPARQL queries</param>
    /// <param name="upload">Use current DB to upload statistics</param>
    /// <param name="questionId">Variable name of "selectQueriesQuery" for question ids</param>
    /// <param name="queryId">Variable name of "selectQueriesQuery" for query ids</param>
    /// <param name="queryText">Variable name of "selectQueriesQuery" for query string</param>
    public StardogSparqlConnector(string stardogBaseUri, string stardogUsername, string stardogPassword,
        string database, string selectQueriesQuery, bool upload, string questionId = "question", string queryId = "query", 
        string queryText = "text")
    {
        _selectQueriesQuery = selectQueriesQuery;
        _connector = new StardogConnector(stardogBaseUri, database, stardogUsername, stardogPassword);
        _questionId = questionId;
        _queryId = queryId;
        _queryText = queryText;
        _upload = upload;
    }
    
    public void UploadStats(string query)
    {
        _connector.Update(query);
    }
    
    public IEnumerable<string> GetQueries()
    {
        var results = (SparqlResultSet) _connector.Query(_selectQueriesQuery);

        return (from sparqlResult in results let queryUri = sparqlResult.Value(_queryId).ToString()
            let queryText = sparqlResult.Value(_queryText).ToString() let questionUri = sparqlResult.Value(_questionId).ToString() select 
                @$"{questionUri}/||\{queryUri}/||\{queryText.Replace('\n', ' ')}").ToList();
    }

    public bool UploadToDb()
    {
        return _upload;
    }
}

/// <summary>
/// Class for analysing SPARQL queries and generating statistics over them
/// </summary>
public static class SparqlParser
{
    /// <summary>
    /// Get number of triples of a SPARQL query
    /// </summary>
    /// <param name="pattern">SPARQL graph pattern to analyse</param>
    /// <returns>Number of unique triples inside a SPARQL query</returns>
    private static int GetTriples(GraphPattern pattern)
    {
        // get all triples of all child graphs
        var triples = pattern.ChildGraphPatterns.Sum(GetTriples);

        // foreach triple in current graph
        foreach (var triple in pattern.TriplePatterns)
        {
            switch (triple)
            {
                // triple is a normal triple
                case not IFilterPattern and not SubQueryPattern:
                    triples++;
                    break;
                // triple is a sub query
                case SubQueryPattern subQueryPattern:
                    triples += GetTriples(subQueryPattern.SubQuery.RootGraphPattern);
                    break;
            }
        }

        return triples;
    }

    /// <summary>
    /// Get number of filters applied on a SPARQL query
    /// </summary>
    /// <param name="pattern">SPARQL graph pattern to analyse</param>
    /// <returns>number of FILTER statements inside a SPARQL query</returns>
    private static int GetFilters(GraphPattern pattern)
    {
        // get number of filters for all child graphs
        var filters = pattern.ChildGraphPatterns.Sum(GetFilters);

        // foreach triple in current graph
        foreach (var triplePattern in pattern.TriplePatterns)
        {
            switch (triplePattern)
            {
                // triple is a FILTER statement
                case FilterPattern:
                    filters++;
                    break;
                // triple is a sub query
                case SubQueryPattern subQueryPattern:
                    filters += GetFilters(subQueryPattern.SubQuery.RootGraphPattern);
                    break;
            }
        }
        
        return filters;
    }

    /// <summary>
    /// Get number of resources used inside a SPARQL query (subjects/predicates/objects that aren't variables)
    /// </summary>
    /// <param name="pattern">SPARQL graph pattern to analyse</param>
    /// <param name="predicates">true, if predicates as resources should be counted, else false</param>
    /// <returns>Number of resources used in a SPARQL query</returns>
    private static int GetResources(GraphPattern pattern, bool predicates)
    {
        // Get resources of all child graphs
        var resources = pattern.ChildGraphPatterns.Sum(childPattern => GetResources(childPattern, predicates));

        // foreach triple in current graph
        foreach (var triplePattern in pattern.TriplePatterns)
        {
            // triple is a sub query
            if (triplePattern is SubQueryPattern subQueryPattern)
            {
                // count resources of sub query
                resources += GetResources(subQueryPattern.SubQuery.RootGraphPattern, predicates);
                continue;
            }
            
            // triple is no normal triple (e. g. BIND, FILTER, ...)
            if (triplePattern is not TriplePattern triple) continue;
        
            // subject is a resource
            if (triple.Subject is NodeMatchPattern)
            {
                resources++;
            }

            // predicate is a resource
            if (predicates && triple.Predicate is NodeMatchPattern)
            {
                resources++;
            }

            // object is a resource
            if (triple.Object is NodeMatchPattern)
            {
                resources++;
            }
        }

        return resources;
    }

    /// <summary>
    /// Get number of ORDER BY statements used in a SPARQL query
    /// </summary>
    /// <param name="query">SPARQL query</param>
    /// <returns>number of ORDER BY statements</returns>
    private static int GetOrderBys(SparqlQuery query)
    {
        int OrderBysSubQuery(GraphPattern pattern)
        {
            // Get ORDER BY statements of all child graphs
            var subQueryOrderBys = pattern.ChildGraphPatterns.Sum(OrderBysSubQuery);

            // foreach triple in current subgraph
            foreach (var triplePattern in pattern.TriplePatterns)
            {
                // triple is a sub query
                if (triplePattern is SubQueryPattern subQueryPattern)
                {
                    subQueryOrderBys += GetOrderBys(subQueryPattern.SubQuery);
                }
            }

            return subQueryOrderBys;
        }
        
        // Get number of ORDER BY statements from possible sub queries
        var orderBys = OrderBysSubQuery(query.RootGraphPattern);
        
        // Current query has an ORDER BY statement
        if (query.OrderBy is not null)
        {
            orderBys++;
        }

        return orderBys;
    }

    /// <summary>
    /// Get number of LIMIT statements of a SPARQL query
    /// </summary>
    /// <param name="query">SPARQL query</param>
    /// <returns>number of LIMIT statements</returns>
    private static int GetLimits(SparqlQuery query)
    {
        // get number of LIMIT statements of a sub query
        int LimitsSubQuery(GraphPattern pattern)
        {
            // get number of LIMIT statements of each child graph
            var subQueryLimits = pattern.ChildGraphPatterns.Sum(LimitsSubQuery);

            // 
            foreach (var triplePattern in pattern.TriplePatterns)
            {
                if (triplePattern is SubQueryPattern subQueryPattern)
                {
                    subQueryLimits += GetLimits(subQueryPattern.SubQuery);
                }
            }

            return subQueryLimits;
        }

        // get number of LIMIT statements of all possible sub queries
        var limits = LimitsSubQuery(query.RootGraphPattern);

        // current query uses a LIMIT expression
        if (query.Limit >= 0)
        {
            limits++;
        }

        return limits;
    }

    /// <summary>
    /// Get number of HAVING statements inside a SPARQL query
    /// </summary>
    /// <param name="query">SPARQL query</param>
    /// <returns>number of HAVING statements</returns>
    private static int GetHaving(SparqlQuery query)
    {
        // get number of HAVING statements for all sub queries
        int HavingSubQuery(GraphPattern pattern)
        {
            // get number of HAVING statements of all child graphs
            var havingSubQuery = pattern.ChildGraphPatterns.Sum(HavingSubQuery);

            // foreach triple in current graph
            foreach (var triplePattern in pattern.TriplePatterns)
            {
                // triple is a sub query
                if (triplePattern is SubQueryPattern subQueryPattern)
                {
                    havingSubQuery += GetHaving(subQueryPattern.SubQuery);
                }
            }

            return havingSubQuery;
        }
        
        // number of HAVING statements in possible sub queries
        var having = HavingSubQuery(query.RootGraphPattern);

        // current query has a HAVING statement
        if (query.Having is not null)
        {
            having++;
        }

        return having;
    }

    /// <summary>
    /// Get number of OFFSET statements in a SPARQL query
    /// </summary>
    /// <param name="query">SPARQL query</param>
    /// <returns>number of OFFSET statements in a SPARQL query</returns>
    private static int GetOffsets(SparqlQuery query)
    {
        // get number of OFFSET statements in a sub query
        int OffsetsSubQuery(GraphPattern pattern)
        {
            // get number of OFFSET statements in all child graphs
            var offsetsSubQuery = pattern.ChildGraphPatterns.Sum(OffsetsSubQuery);
            
            // foreach triple in the current sub query
            foreach (var triplePattern in pattern.TriplePatterns)
            {
                // triple is a sub query
                if (triplePattern is SubQueryPattern subQueryPattern)
                {
                    offsetsSubQuery += GetOffsets(subQueryPattern.SubQuery);
                }
            }

            return offsetsSubQuery;
        }

        // number of OFFSET statements in possible sub queries
        var offsets = OffsetsSubQuery(query.RootGraphPattern);

        // current query has an OFFSET statement
        if (query.Offset > 0)
        {
            offsets++;
        }
        
        return offsets;
    }

    /// <summary>
    /// Get number of GROUP BY statements in a SPARQL query
    /// </summary>
    /// <param name="query">SPARQL query</param>
    /// <returns>number of GROUP BY statements</returns>
    private static int GetGroupBys(SparqlQuery query)
    {
        // get number of GROUP BY statements of a sub query
        int GroupBysSubQuery(GraphPattern pattern)
        {
            // get number of GROUP BY statements of all child graphs
            var groupBysSubQuery = pattern.ChildGraphPatterns.Sum(GroupBysSubQuery);

            // foreach triple in current graph
            foreach (var triplePattern in pattern.TriplePatterns)
            {
                // triple is a sub query
                if (triplePattern is SubQueryPattern subQueryPattern)
                {
                    groupBysSubQuery += GetGroupBys(subQueryPattern.SubQuery);
                }
            }
            
            return groupBysSubQuery;
        }

        // number of GROUP BY statements of all possible sub queries
        var groupBys = GroupBysSubQuery(query.RootGraphPattern);

        // current query has a GROUP BY statement
        if (query.GroupBy is not null)
        {
            groupBys++;
        }

        return groupBys;
    }

    private static int GetQueryLength(SparqlQuery query)
    {
        var queryString = query.ToString();
        var namespaces = query.NamespaceMap;

        foreach (var prefix in namespaces.Prefixes)
        {
            var fullUri = namespaces.GetNamespaceUri(prefix);
            var uriReplace = $" {fullUri}";
            queryString = queryString.Replace($" {prefix}:", uriReplace);
            queryString = Regex.Replace(queryString, @$"{uriReplace}\w+", "<urn:placeholder>");
            queryString = queryString.Replace($"PREFIX {fullUri} <{fullUri}>", "");

            if (prefix == "")
            {
                queryString = queryString.Replace($"BASE <{fullUri}>", "");
            }
        }

        return queryString.Replace("\n", "").Length;
    }
    
    /// <summary>
    /// Analyses a SPARQL query and returns a Dictionary with its statistics
    /// </summary>
    /// <param name="query">SPARQL query to analyse</param>
    /// <returns>Dictionary with statistics of the SPARQL query</returns>
    public static Dictionary<string, int> AnalyseQuery(SparqlQuery query)
    {
        var modifierStats = new Dictionary<string, int>
            {
                ["urn:qado#numberOfModifierOrderBy"] = GetOrderBys(query),
                ["urn:qado#numberOfModifierLimit"] = GetLimits(query),
                ["urn:qado#numberOfModifierHaving"] = GetHaving(query),
                ["urn:qado#numberOfModifierOffset"] = GetOffsets(query),
                ["urn:qado#numberOfModifierGroupBy"] = GetGroupBys(query)
            };

        modifierStats["urn:qado#numberOfModifiers"] = modifierStats.Values.Sum();
        
        var queryStats = new Dictionary<string, int>
        {
            ["urn:qado#numberOfTriples"] = GetTriples(query.RootGraphPattern),
            ["urn:qado#numberOfFilters"] = GetFilters(query.RootGraphPattern),
            ["urn:qado#numberOfVariables"] = query.Variables.Count(),
            ["urn:qado#numberOfResources"] = GetResources(query.RootGraphPattern, true),
            ["urn:qado#normalizedQueryLength"] = GetQueryLength(query),
            ["urn:qado#numberOfResourcesSubjectsObjects"] = GetResources(query.RootGraphPattern, false)
        };

        queryStats["urn:qado#numberOfResourcesPredicates"] = queryStats["urn:qado#numberOfResources"] -
                                                                     queryStats["urn:qado#numberOfResourcesSubjectsObjects"];

        foreach (var entry in modifierStats)
        {
            queryStats.Add(entry.Key, entry.Value);
        }

        return queryStats;
    }
}

public enum DbType
{
    Unknown = -1, Stardog
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

    /// <summary>
    /// Construct a IDbConnector from the given settings
    /// </summary>
    /// <returns>IDbConnector with the given settings</returns>
    /// <exception cref="InvalidEnumArgumentException">An invalid setting has been provided</exception>
    public IDbConnector Construct()
    {
        return Type switch
        {
            DbType.Stardog => new StardogSparqlConnector($"{DatabaseUri}:{DatabasePort}", Username, Password,
                DatabaseName, SelectQuery, Upload),
            DbType.Unknown => throw new InvalidEnumArgumentException("Select a proper DB type"),
            _ => throw new InvalidEnumArgumentException("Invalid DB type selected")
        };
    }
}

public class SparqlAnalysisState
{
    public AnalysisState State { get; set; } = AnalysisState.Initialized;
    public string CreatedAt { get; set; } = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);
    public string? FinishedAt { get; set; }
    public string Id { get; set; } = Guid.NewGuid().ToString();
}

public enum AnalysisState
{
    Initialized, Running, Failed, Finished
}

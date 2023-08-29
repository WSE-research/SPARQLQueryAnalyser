﻿using System.ComponentModel;
using System.Globalization;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using VDS.RDF;
using VDS.RDF.Query;
using VDS.RDF.Query.Paths;
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
    /// <param name="query">INSERT query for triplestore</param>
    public void UploadStats(string query);
    
    /// <summary>
    /// Load all queries from DB and build a list with them.
    /// </summary>
    /// <returns>List of strings in the format "QuestionID/||\QueryID/||\SPARQLQuery"</returns>
    public IEnumerable<string> GetQueries();
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

        if (pattern.InlineData is not null)
        {
            resources += pattern.InlineData.Tuples.Count();    
        }

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

            switch (triplePattern)
            {
                // triple is no normal triple (e. g. BIND, FILTER, ...)
                case TriplePattern triple:
                {
                    resources += GetSubjectObjects(triplePattern);
                    
                    // predicate is a resource
                    if (predicates && triple.Predicate is NodeMatchPattern)
                    {
                        resources++;
                    }

                    break;
                }
                case PropertyPathPattern pathPattern:
                    resources += GetSubjectObjects(triplePattern);
                    if (predicates)
                    {
                        if (pathPattern.Path is BaseUnaryPath path)
                        {
                            resources += TraversePath(path);
                        }
                        else
                        {
                            resources += TraversePath((BaseBinaryPath) pathPattern.Path);  
                        }
                    }
                    break;
            }
        }

        return resources;
    }

    private static int TraversePath(BaseBinaryPath path)
    {
        var resources = 0;

        if (path.RhsPath is Property { Predicate: UriNode })
        {
            resources++;
        }

        switch (path.LhsPath)
        {
            case Property { Predicate: UriNode }:
                return ++resources;
            case BaseBinaryPath leftPath:
                resources += TraversePath(leftPath);
                break;
        }

        return resources;
    }

    private static int TraversePath(BaseUnaryPath path)
    {
        var resources = 0;

        if (path.Path is Property { Predicate: UriNode })
        {
            resources++;
        }
        
        return resources;
    }

    private static int GetSubjectObjects(dynamic triple)
    {
        var resources = 0;
        
        // subject is a resource
        if (triple.Subject is NodeMatchPattern)
        {
            resources++;
        }

        // object is a resource
        if (triple.Object is NodeMatchPattern)
        {
            resources++;
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
            queryString = queryString!.Replace($" {prefix}:", uriReplace);
            queryString = Regex.Replace(queryString, @$"{uriReplace}\w+", "<urn:placeholder>");
            queryString = queryString.Replace($"PREFIX {fullUri} <{fullUri}>", "");

            if (prefix == "")
            {
                queryString = queryString.Replace($"BASE <{fullUri}>", "");
            }
        }

        return queryString!.Replace("\n", "").Length;
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
                ["http://purl.com/qado/ontology.ttl#numberOfModifierOrderBy"] = GetOrderBys(query),
                ["http://purl.com/qado/ontology.ttl#numberOfModifierLimit"] = GetLimits(query),
                ["http://purl.com/qado/ontology.ttl#numberOfModifierHaving"] = GetHaving(query),
                ["http://purl.com/qado/ontology.ttl#numberOfModifierOffset"] = GetOffsets(query),
                ["http://purl.com/qado/ontology.ttl#numberOfModifierGroupBy"] = GetGroupBys(query)
            };

        modifierStats["http://purl.com/qado/ontology.ttl#numberOfModifiers"] = modifierStats.Values.Sum();
        
        var queryStats = new Dictionary<string, int>
        {
            ["http://purl.com/qado/ontology.ttl#numberOfTriples"] = GetTriples(query.RootGraphPattern),
            ["http://purl.com/qado/ontology.ttl#numberOfFilters"] = GetFilters(query.RootGraphPattern),
            ["http://purl.com/qado/ontology.ttl#numberOfVariables"] = query.Variables.Count(),
            ["http://purl.com/qado/ontology.ttl#numberOfResources"] = GetResources(query.RootGraphPattern, true),
            ["http://purl.com/qado/ontology.ttl#normalizedQueryLength"] = GetQueryLength(query),
            ["http://purl.com/qado/ontology.ttl#numberOfResourcesSubjectsObjects"] = GetResources(query.RootGraphPattern, false)
        };

        queryStats["http://purl.com/qado/ontology.ttl#numberOfResourcesPredicates"] = queryStats["http://purl.com/qado/ontology.ttl#numberOfResources"] -
                                                                     queryStats["http://purl.com/qado/ontology.ttl#numberOfResourcesSubjectsObjects"];

        foreach (var entry in modifierStats)
        {
            queryStats.Add(entry.Key, entry.Value);
        }

        return queryStats;
    }
}

public enum DbType
{
    Unknown = -1, Stardog, GraphDb
}

public class DatabaseConfig
{
    /// <summary>
    /// Class providing SPARQL queries stored with Stardog (https://www.stardog.com/)
    /// </summary>
    private class StardogSparqlConnector: IDbConnector
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
        internal StardogSparqlConnector(string stardogBaseUri, string stardogUsername, string stardogPassword,
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
                let queryText = ((LiteralNode)sparqlResult.Value(_queryText)).Value let questionUri = sparqlResult.Value(_questionId).ToString() select 
                    @$"{questionUri}/||\{queryUri}/||\{queryText.Replace('\n', ' ')}").ToList();
        }

        public bool UploadToDb()
        {
            return _upload;
        }
    }
    
    /// <summary>
    /// Connector class for GraphDb instances (https://www.ontotext.com/products/graphdb/)
    /// </summary>
    private class GraphDbConnector : IDbConnector
    {
        private readonly bool _upload;
        private readonly string _repositoryUri;
        private readonly string _selectQuery;
        private readonly string _questionId;
        private readonly string _queryId;
        private readonly string _queryText;
        
        internal GraphDbConnector(string dbUri, string username, string password, string database, string selectQuery,
            bool upload, string questionId = "question", string queryId = "query", string queryText = "text")
        {
            // dbUri = dbUri.Replace("://", $"://{username}:{password}@");
            
            _repositoryUri = $"{dbUri}/repositories/{database}";
            _upload = upload;
            _selectQuery = selectQuery;
            _questionId = questionId;
            _queryId = queryId;
            _queryText = queryText;
        }
        
        public bool UploadToDb()
        {
            return _upload;
        }

        public void UploadStats(string query)
        {
            var insertRequest = $"{_repositoryUri}/statements?update=" + Uri.EscapeDataString(query.Replace("\n", " "));
            
            var client = new HttpClient{Timeout = new TimeSpan(0, 0, 0, 60)};

            try
            {
                var response = client.PostAsync(insertRequest, null);
                response.Wait();
            }
            catch (InvalidOperationException e)
            {
                Console.Error.WriteLine("Invalid Operation");
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
            }
            catch (HttpRequestException e)
            {
                Console.Error.WriteLine("HTTP Request failed");
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
            }
            catch (TaskCanceledException e)
            {
                Console.Error.WriteLine("Request canceled by main method");
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
            }
            catch (UriFormatException e)
            {
                Console.Error.WriteLine("Invalid URI format");
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
            }
            catch (AggregateException e)
            {
                Console.Error.WriteLine("Exception while execution occured");
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);

                foreach (var innerException in e.InnerExceptions)
                {
                    Console.Error.WriteLine("Inner Exception: {0}", innerException.GetType());
                    Console.Error.WriteLine(innerException.Message);
                    Console.Error.WriteLine(innerException.StackTrace);

                    if (innerException.InnerException == null) continue;
                    
                    Console.Error.WriteLine(innerException.InnerException.Message);
                    Console.Error.WriteLine(innerException.InnerException.StackTrace);
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("General exception caused");
                Console.Error.WriteLine("Exception type: {0}", e.GetType());
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(e.StackTrace);
            }
        }

        public IEnumerable<string> GetQueries()
        {
            var client = new HttpClient();
            client.DefaultRequestHeaders.Accept.Clear();
            client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/sparql-results+json"));
            var selectRequestUri = $"{_repositoryUri}?query=" + Uri.EscapeDataString(_selectQuery);
            var request = client.GetStringAsync(selectRequestUri);
            request.Wait();
            var result = request.Result;
            var sparqlResult = (JObject)JsonConvert.DeserializeObject(result)!;

            var queries = new LinkedList<string>();

            foreach (var binding in sparqlResult["results"]?["bindings"]!)
            {
                var currentQueryId = binding[_queryId]?["value"]?.Value<string>();
                var currentQuestionId = binding[_questionId]?["value"]?.Value<string>();
                var currentQuery = binding[_queryText]?["value"]?.Value<string>()?.Replace('\n', ' ');
                
                queries.AddLast(@$"{currentQuestionId}/||\{currentQueryId}/||\{currentQuery}");
            }

            return queries;
        }
    }
    
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
        var dbFullUri = $"{DatabaseUri}:{DatabasePort}";
        
        return Type switch
        {
            DbType.Stardog => new StardogSparqlConnector(dbFullUri, Username, Password, DatabaseName, SelectQuery, Upload),
            DbType.GraphDb => new GraphDbConnector(dbFullUri, Username, Password, DatabaseName, SelectQuery, Upload),
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

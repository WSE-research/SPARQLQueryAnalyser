using VDS.RDF.Query;
using VDS.RDF.Query.Patterns;
using VDS.RDF.Storage;

namespace SPARQLAnalyser;

/// <summary>
/// Class providing SPARQL queries stored with Stardog
/// </summary>
public class StardogSparqlConnector
{
    private readonly StardogConnector _connector;
    private readonly string _selectQueriesQuery;

    /// <summary>
    /// Initialize connection to a Stardog instance
    /// </summary>
    /// <param name="stardogBaseUri">BaseURI for the used Stardog instance (e. g. http://localhost:5820)</param>
    /// <param name="stardogUsername">Stardog username</param>
    /// <param name="stardogPassword">Stardog password</param>
    /// <param name="database">Database used as query storage</param>
    /// <param name="selectQueriesQuery">Query used to get the stored SPARQL queries</param>
    public StardogSparqlConnector(string stardogBaseUri, string stardogUsername, string stardogPassword,
        string database, string selectQueriesQuery)
    {
        _selectQueriesQuery = selectQueriesQuery;
        _connector = new StardogConnector(stardogBaseUri, database, stardogUsername, stardogPassword);
    }

    /// <summary>
    /// Execute an INSERT query for the statistics generated for the managed SPARQL queries
    /// </summary>
    /// <param name="query">INSERT query for stardog database</param>
    public void UploadStats(string query)
    {
        _connector.Update(query);
    }

    /// <summary>
    /// Get an iterator over the fetched SPARQL queries
    /// </summary>
    /// <returns>SparqlResultSet with all selected SPARQL queries</returns>
    public SparqlResultSet GetQueries()
    {
        return (SparqlResultSet) _connector.Query(_selectQueriesQuery);
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
    /// <returns>Number of resources used in a SPARQL query</returns>
    private static int GetResources(GraphPattern pattern)
    {
        // Get resources of all child graphs
        var resources = pattern.ChildGraphPatterns.Sum(GetResources);

        // foreach triple in current graph
        foreach (var triplePattern in pattern.TriplePatterns)
        {
            // triple is a sub query
            if (triplePattern is SubQueryPattern subQueryPattern)
            {
                // count resources of sub query
                resources += GetResources(subQueryPattern.SubQuery.RootGraphPattern);
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
            if (triple.Predicate is NodeMatchPattern)
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
        int GetOrderBysSubQuery(GraphPattern pattern)
        {
            // Get ORDER BY statements of all child graphs
            var subQueryOrderBys = pattern.ChildGraphPatterns.Sum(GetOrderBysSubQuery);

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
        var orderBys = GetOrderBysSubQuery(query.RootGraphPattern);
        
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
        int GetLimitsSubQuery(GraphPattern pattern)
        {
            // get number of LIMIT statements of each child graph
            var subQueryLimits = pattern.ChildGraphPatterns.Sum(GetLimitsSubQuery);

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
        var limits = GetLimitsSubQuery(query.RootGraphPattern);

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
        int GetHavingSubQuery(GraphPattern pattern)
        {
            // get number of HAVING statements of all child graphs
            var havingSubQuery = pattern.ChildGraphPatterns.Sum(GetHavingSubQuery);

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
        var having = GetHavingSubQuery(query.RootGraphPattern);

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
        int GetOffsetsSubQuery(GraphPattern pattern)
        {
            // get number of OFFSET statements in all child graphs
            var offsetsSubQuery = pattern.ChildGraphPatterns.Sum(GetOffsetsSubQuery);
            
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
        var offsets = GetOffsetsSubQuery(query.RootGraphPattern);

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
        int GetGroupBysSubQuery(GraphPattern pattern)
        {
            // get number of GROUP BY statements of all child graphs
            var groupBysSubQuery = pattern.ChildGraphPatterns.Sum(GetGroupBysSubQuery);

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
        var groupBys = GetGroupBysSubQuery(query.RootGraphPattern);

        // current query has a GROUP BY statement
        if (query.GroupBy is not null)
        {
            groupBys++;
        }

        return groupBys;
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
                ["urn:qa:benchmark#numberOfModifierOrderBy"] = GetOrderBys(query),
                ["urn:qa:benchmark#numberOfModifierLimit"] = GetLimits(query),
                ["urn:qa:benchmark#numberOfModifierHaving"] = GetHaving(query),
                ["urn:qa:benchmark#numberOfModifierOffset"] = GetOffsets(query),
                ["urn:qa:benchmark#numberOfModifierGroupBy"] = GetGroupBys(query)
            };

        modifierStats["urn:qa:benchmark#numberOfModifiers"] = modifierStats.Values.Sum();
        
        var queryStats = new Dictionary<string, int>
        {
            ["urn:qa:benchmark#numberOfTriples"] = GetTriples(query.RootGraphPattern),
            ["urn:qa:benchmark#numberOfFilters"] = GetFilters(query.RootGraphPattern),
            ["urn:qa:benchmark#numberOfVariables"] = query.Variables.Count(),
            ["urn:qa:benchmark#numberOfResources"] = GetResources(query.RootGraphPattern)
        };

        foreach (var entry in modifierStats)
        {
            queryStats.Add(entry.Key, entry.Value);
        }

        return queryStats;
    }
}

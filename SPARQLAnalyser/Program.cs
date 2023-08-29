using System.Globalization;
using System.Text;
using VDS.RDF;
using VDS.RDF.Parsing;
using VDS.RDF.Storage;
using VDS.RDF.Writing.Formatting;
using SPARQLParser;
using System.Text.Json;
using VDS.RDF.Query;

const int batchSize = 2;
var analysisPath = Environment.GetEnvironmentVariable("docker-analysis-path");

// custom prefixes for analysed SPARQL queries
var prefixDictionary = JsonSerializer.Deserialize<Dictionary<string, string>>(File.ReadAllText("prefixes.json"));
var basePath = analysisPath ?? "analysis";

var statisticsPath = Path.Join(basePath, "statistics.json");

// prefixes for the insert statement
const string qado = "http://purl.com/qado/ontology.ttl#";

// query parser and Stardog connection
var queryParser = new SparqlQueryParser(SparqlQuerySyntax.Extended);

var dbConfig = JsonSerializer.Deserialize<DatabaseConfig>(File.ReadAllText(Path.Join(basePath, "connector.json")));
var queryReader = dbConfig?.Construct();
//var queries = File.ReadAllLines(Path.Join(basePath, "queries"));
var queries = queryReader.GetQueries().ToArray();
var state = JsonSerializer.Deserialize<SparqlAnalysisState>(File.ReadAllText(Path.Join(basePath, "state.json")));
var statistics = JsonSerializer.Deserialize<SparqlAnalysisStatistics>(File.ReadAllText(statisticsPath));

if (state is null) return;

state.State = AnalysisState.Running;
statistics!.State = state;

File.WriteAllText(statisticsPath, JsonSerializer.Serialize(statistics));

var parsable = queries.Length;
var nonParsable = 0;

var questions = new Dictionary<string, int>();
var parseExceptions = new HashSet<string>();
var benchmarkPrefix = new Dictionary<string, HashSet<Uri>>();

var count = 0;

var triples = new List<Triple>();

var insert = new StringBuilder();
insert.Append("PREFIX qado: <").Append(qado).Append(">\nINSERT{\n");

var batch = 0;

Console.WriteLine("Generating Triples for SPARQL queries...");

foreach (var queryString in queries)
{
    var g = new Graph();
    g.NamespaceMap.AddNamespace("qado", new Uri(qado));
    
    // get SPARQL query, question ID and benchmark name
    var queryText = queryString.Split(@"/||\").Last().Replace(" OR ", " || ") ;
    var question = queryString.Split(@"/||\").First();
    var queryUri = queryString.Split(@"/||\").Skip(1).First();
    var benchmarkDataset = question.Split("-question").First();

    try
    {
        var prefixes = new StringBuilder();
        
        SparqlQuery query;
        while (true)
        {
            try
            {
                // parse query
                var queryBuilder = new StringBuilder().Append(prefixes.ToString()).Append(queryText);
                query = queryParser.ParseFromString(queryBuilder.ToString());
                break;
            }
            catch (RdfException e)
            {
                // missing PREFIX provided by original knowledge graph
                if (e.Message.Contains("The Namespace URI for the given Prefix"))
                {
                    // get prefix name and uri
                    var prefix = e.Message.Split('\u0027').Skip(1).First();
                    var prefixUri = prefixDictionary?[prefix];

                    // add prefix to query string
                    prefixes.Append($"PREFIX {prefix}: <{prefixUri}>\n");
                    continue;
                }

                // missing BASE uri
                if (e.Message.Contains("there is no in-scope Base URI!"))
                {
                    // adding BASE uri to query string
                    var baseUri = prefixDictionary?["base"];
                    
                    prefixes.Append($"PREFIX : <{baseUri}>\nBASE <{baseUri}>\n");
                    continue;
                }

                throw;
            }
        }

        // get statistics for the current query
        var queryStats = SparqlParser.AnalyseQuery(query);

        // initialize Set if benchmark is new
        if (!benchmarkPrefix.ContainsKey(benchmarkDataset))
        {
            benchmarkPrefix[benchmarkDataset] = new HashSet<Uri>();
        }

        // storing all prefixes used by this query
        foreach (var prefix in query.NamespaceMap.Prefixes)
        {
            benchmarkPrefix[benchmarkDataset].Add(query.NamespaceMap.GetNamespaceUri(prefix));
        }

        // get query type
        var queryType = query.QueryType.ToString();

        // create triples for the INSERT query with the statistics
        triples.AddRange(queryStats.Select(modifierEntry => new Triple(g.CreateUriNode(new Uri(queryUri)), 
            g.CreateUriNode(new Uri(modifierEntry.Key)), g.CreateLiteralNode(modifierEntry.Value.ToString(), 
                new Uri(XmlSpecsHelper.XmlSchemaDataTypeNonNegativeInteger)))));
        
        triples.Add(new Triple(g.CreateUriNode(new Uri(queryUri)), g.CreateUriNode(new Uri("http://purl.com/qado/ontology.ttl#queryType")),
            g.CreateLiteralNode(queryType)));

        // batch parsed
        if (++count % batchSize == 0)
        {
            // add all triples to INSERT query
            foreach (var triple in triples)
            {
                insert.Append(triple.ToString(new TurtleFormatter())).Append('\n');
            }

            // finalize query
            insert.Append("} WHERE {}");

            batch = count / batchSize;
            
            // execute query
            while (true)
            {
                try
                {
                    // run INSERT query and reset it for next batch
                    if (queryReader is not null && queryReader.UploadToDb())
                    {
                        queryReader.UploadStats(insert.ToString());
                    }
                    
                    File.WriteAllText(Path.Join(basePath, $"{batch}.sparql"), insert.ToString());

                    triples.Clear();
                    insert.Clear().Append("PREFIX qado: <").Append(qado).Append(">\nINSERT {\n");
                    break;
                }
                // query execution killed before finishing
                catch (RdfStorageException) { }
            }
        }
    }
    // exceptions while parsing
    catch (Exception e) when (e is RdfParseException or RdfException or NullReferenceException)
    {
        // store error message with question id
        parseExceptions.Add( queryUri + "    " + e.Message);

        // count not parsable questions per benchmark
        if (questions.ContainsKey(benchmarkDataset))
        {
            questions[benchmarkDataset]++;    
        }
        else
        {
            questions[benchmarkDataset] = 1;
        }

        nonParsable++;
    }
}

// finalize last batch
foreach (var triple in triples)
{
    insert.Append(triple.ToString(new NTriplesFormatter())).Append('\n');
}

insert.Append("} WHERE {}");

// run last INSERT query
while (triples.Count != 0)
{
    try
    {
        if (queryReader is not null && queryReader.UploadToDb())
        {
            queryReader.UploadStats(insert.ToString());    
        }
        
        File.WriteAllText(Path.Join(basePath, $"{++batch}.sparql"), insert.ToString());
        triples.Clear();
    }
    catch(RdfStorageException) {}
}

foreach (var benchmark in benchmarkPrefix.Keys)
{
    insert.Clear().Append("PREFIX qado: <").Append(qado).Append(">\nINSERT {\n");
    
    if (benchmarkPrefix[benchmark].Count == 0)
    {
        benchmarkPrefix[benchmark].Add(new Uri("urn:no:prefix"));
    }
    
    foreach (var prefixUri in benchmarkPrefix[benchmark])
        insert.Append("<").Append(benchmark).Append("-dataset> qado:hasPrefix <").Append(prefixUri).Append("> .\n");
    
    insert.Append("} WHERE {}");

    Console.WriteLine($"Insert benchmark {benchmark} prefixes...");

    while (true)
    {
        try
        {
            if (queryReader is not null && queryReader.UploadToDb())
            {
                queryReader.UploadStats(insert.ToString());    
            }
        
            File.WriteAllText(Path.Join(basePath, $"{++batch}.sparql"), insert.ToString());
            break;
        }
        catch(RdfStorageException) {}
    }
}

statistics.Parsable = parsable - nonParsable;
statistics.NonParsable = nonParsable;

foreach (var exception in parseExceptions)
{
    statistics.Exceptions.Add(exception);
}

state.FinishedAt = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);
state.State = AnalysisState.Finished;
statistics.State = state;

File.WriteAllText(statisticsPath, JsonSerializer.Serialize(statistics, new JsonSerializerOptions
{
    WriteIndented = true
}));

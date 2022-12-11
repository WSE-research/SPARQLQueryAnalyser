using System.Text;
using VDS.RDF;
using VDS.RDF.Parsing;
using VDS.RDF.Storage;
using VDS.RDF.Writing.Formatting;
using SPARQLParser;
using System.Text.Json;

const int batchSize = 2048;

// custom prefixes for analysed SPARQL queries
var prefixes = File.ReadAllText("prefixes.sparql");
var statePath = Path.Join("analyse", "state.json");

// prefixes for the insert statement
const string qab = "urn:qa:benchmark#";

// query parser and Stardog connection
var queryParser = new SparqlQueryParser(SparqlQuerySyntax.Extended);

var queryReader = JsonSerializer.Deserialize<IDbConnector>(File.ReadAllText(Path.Join("analyse", "connector.json")));
var queries = JsonSerializer.Deserialize<List<string>>(File.ReadAllText(Path.Join("analyse", "queries.json")));
var state = JsonSerializer.Deserialize<SparqlAnalysisState>(File.ReadAllText(statePath));

if (queries is null || state is null) return;

state.State = AnalysisState.Running;
File.WriteAllText(statePath, JsonSerializer.Serialize(state));

var parsable = 0;
var nonParsable = 0;

var questions = new Dictionary<string, int>();
var allQuestions = new Dictionary<string, int>();
var parseExceptions = new HashSet<string>();

var count = 0;

var triples = new List<Triple>();

var insert = new StringBuilder();
insert.Append("PREFIX qab: <").Append(qab).Append(">\nINSERT{\n");

var batch = 0;

foreach (var queryString in queries)
{
    var g = new Graph();
    g.NamespaceMap.AddNamespace("qab", new Uri(qab));
    
    // get SPARQL query, question ID and benchmark name
    var queryText = queryString.Split(@"/||\").Last().Replace(" OR ", " || ") ;
    var question = queryString.Split(@"/||\").First();
    var benchmarkDataset = question.Split("-question").First();

    // count questions per benchmark
    if (allQuestions.ContainsKey(benchmarkDataset))
    {
        allQuestions[benchmarkDataset]++;
    }
    else
    {
        allQuestions[benchmarkDataset] = 1;
    }
    
    try
    {
        // parse query and optimize it
        var query = queryParser.ParseFromString($"{prefixes} {queryText}");
        query.Optimise();
        
        parsable++;

        // get statistics for the current query
        var queryStats = SparqlParser.AnalyseQuery(query);

        // get query type
        var queryType = query.QueryType.ToString();

        // create triples for the INSERT query with the statistics
        triples.AddRange(queryStats.Select(modifierEntry => new Triple(g.CreateUriNode(new Uri(question)), 
            g.CreateUriNode(new Uri(modifierEntry.Key)), g.CreateLiteralNode(modifierEntry.Value.ToString(), 
                new Uri(XmlSpecsHelper.XmlSchemaDataTypeNonNegativeInteger)))));
        
        triples.Add(new Triple(g.CreateUriNode(new Uri(question)), g.CreateUriNode(new Uri("urn:qa:benchmark#queryType")),
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
                    else
                    {
                        File.WriteAllText($"{batch}.sparql", insert.ToString());
                    }

                    triples.Clear();
                    insert.Clear().Append("PREFIX qab: <").Append(qab).Append(">\nINSERT {\n");

                    Console.WriteLine($"Inserted batch {batch}..");
                    break;
                }
                // query execution killed before finishing
                catch (RdfStorageException) { }
            }
        }
    }
    // exceptions while parsing
    catch (Exception e) when (e is RdfParseException or RdfException)
    {
        switch (e)
        {
            // invalid SPARQL query
            case RdfParseException or RdfException:
                parseExceptions.Add( benchmarkDataset + "\t" + e.Message);
                break;
        }
        
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
Console.WriteLine("Inserting last triples...");
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
        else
        {
            File.WriteAllText(Path.Join("analyse", $"{++batch}.sparql"), insert.ToString());
        }
        triples.Clear();
    }
    catch(RdfStorageException) {}
}

var analysisStatistics = new SparqlAnalysisStatistics
{
    Parsable = parsable,
    NonParsable = nonParsable
};

foreach (var exception in parseExceptions)
{
    analysisStatistics.Exceptions.Add(exception);
}

state.State = AnalysisState.Finished;
analysisStatistics.State = state;

File.WriteAllText(Path.Join("analyse", "statistics.json"), JsonSerializer.Serialize(analysisStatistics));

// print number of not parsed questions per benchmark as error messages
foreach (var entry in questions)
{
    Console.Error.WriteLine($"{entry.Key}\t{entry.Value}\t{(float) entry.Value / allQuestions[entry.Key]:F4}");
}

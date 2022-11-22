using System.Text;
using VDS.RDF;
using VDS.RDF.Parsing;
using SPARQLAnalyser;
using VDS.RDF.Storage;
using VDS.RDF.Writing.Formatting;

// stardog connection settings
const string baseUri = "http://demos.swe.htwk-leipzig.de:40100";
const string username = "admin";
const string password = "admin";
const string dbname = "RDFized-datasets";

const int batchSize = 20;

// custom prefixes for analysed SPARQL queries
var prefixes = File.ReadAllText("prefixes.sparql");

// prefixes for the insert statement
const string qab = "urn:qa:benchmark#";

// query parser and Stardog connection
var queryParser = new SparqlQueryParser(SparqlQuerySyntax.Extended);
var queryReader = new StardogSparqlConnector(baseUri, username, password, dbname, 
    File.ReadAllText("GetQueries.sparql"));

var parsable = 0;
var nonParsable = 0;

var questions = new Dictionary<string, int>();
var allQuestions = new Dictionary<string, int>();
var parseExceptions = new HashSet<string>();
var nullReferenceIds = new List<string>();

var count = 0;
var nullPointers = 0;

var triples = new List<Triple>();

var insert = new StringBuilder();
insert.Append("PREFIX qab: <").Append(qab).Append(">\nINSERT{\n");

foreach (var result in queryReader.GetQueries())
{
    var g = new Graph();
    g.NamespaceMap.AddNamespace("qab", new Uri(qab));
    
    // get SPARQL query, question ID and benchmark name
    var queryText = result.Value("text").ToString().Replace(" OR ", " || ");
    var question = result.Value("question").ToString();
    var benchmarkDataset = question.Split("-question")[0];

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

        if (++count % batchSize == 0)
        {
            foreach (var triple in triples)
            {
                insert.Append(triple.ToString(new TurtleFormatter())).Append("\n");
            }

            insert.Append("} WHERE {}");
            
            while (true)
            {
                try
                {
                    queryReader.UploadStats(insert.ToString());
                    triples.Clear();
                    insert.Clear().Append("PREFIX qab: <").Append(qab).Append(">\nINSERT {\n");

                    Console.WriteLine($"Inserted batch {count / batchSize}..");
                    break;
                }
                catch (RdfStorageException) { }
            }
        }
    }
    catch (Exception e) when (e is RdfParseException or RdfException or NullReferenceException)
    {
        switch (e)
        {
            case RdfParseException or RdfException:
                parseExceptions.Add( benchmarkDataset + "\t" + e.Message);
                break;
            case NullReferenceException:
                nullPointers++;
                nullReferenceIds.Add(string.Join('-', question.Split("-").Skip(3)));
                break;
        }
        
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

Console.WriteLine("Inserting last triples...");
foreach (var triple in triples)
{
    insert.Append(triple.ToString(new NTriplesFormatter())).Append("\n");
}

insert.Append("} WHERE {}");

while (triples.Count != 0)
{
    try
    {
        queryReader.UploadStats(insert.ToString());
        triples.Clear();
    }
    catch(RdfStorageException) {}
}

File.WriteAllLines("CWQNullPointers.txt", nullReferenceIds);

Console.WriteLine("\nExceptions");
foreach (var entry in parseExceptions)
{
    Console.Error.WriteLine(entry);
}

Console.WriteLine($"\nParsable:\t{parsable}");
Console.WriteLine("Not parsable:\t" + nonParsable);
Console.WriteLine("Null Pointer exceptions:\t" + nullPointers);

foreach (var entry in questions)
{
    Console.WriteLine($"{entry.Key}\t{entry.Value}\t{(float) entry.Value / allQuestions[entry.Key]:F4}");
}

using SPARQLAnalyser;
using VDS.RDF.Parsing;

namespace SPARQLAnalyserTest;

public class Tests
{
    private readonly SparqlQueryParser _queryParser = new();

    [Test]
    public void BasicTest()
    {
        var stats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("BasicSPARQL.sparql"));
        
        Assert.That(stats["urn:qa:benchmark#numberOfTriples"], Is.EqualTo(1));
        Assert.That(stats["urn:qa:benchmark#numberOfVariables"], Is.EqualTo(2));
        Assert.That(stats["urn:qa:benchmark#numberOfResources"], Is.EqualTo(1));
        Assert.That(stats["urn:qa:benchmark#numberOfModifierLimit"], Is.EqualTo(0));
    }

    [Test]
    public void SubQueryTest()
    {
        var stats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("SubQuery.sparql"));
        
        Assert.That(stats["urn:qa:benchmark#numberOfTriples"], Is.EqualTo(4));
        Assert.That(stats["urn:qa:benchmark#numberOfVariables"], Is.EqualTo(5));
        Assert.That(stats["urn:qa:benchmark#numberOfResources"], Is.EqualTo(6));
        Assert.That(stats["urn:qa:benchmark#numberOfModifierOrderBy"], Is.EqualTo(2));
        Assert.That(stats["urn:qa:benchmark#numberOfFilters"], Is.EqualTo(1));
    }

    [Test]
    public void GroupByTest()
    {
        var stats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("GroupBy.sparql"));
        
        Assert.That(stats["urn:qa:benchmark#numberOfModifierGroupBy"], Is.EqualTo(1));
        Assert.That(stats["urn:qa:benchmark#numberOfModifierHaving"], Is.EqualTo(1));
    }
}
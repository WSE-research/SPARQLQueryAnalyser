using SPARQLParser;
using VDS.RDF.Parsing;

namespace SPARQLAnalyserTest;

public class Tests
{
    private readonly SparqlQueryParser _queryParser = new();

    [Test]
    public void BasicTest()
    {
        var stats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("BasicSPARQL.sparql"));
        Assert.Multiple(() =>
        {
            Assert.That(stats["http://purl.com/qado/ontology.ttl#normalizedQueryLength"], Is.EqualTo(88));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfTriples"], Is.EqualTo(1));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfVariables"], Is.EqualTo(2));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfResources"], Is.EqualTo(1));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfModifierLimit"], Is.EqualTo(0));
        });
    }

    [Test]
    public void SubQueryTest()
    {
        var stats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("SubQuery.sparql"));
        Assert.Multiple(() =>
        {
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfTriples"], Is.EqualTo(4));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfVariables"], Is.EqualTo(5));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfResources"], Is.EqualTo(6));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfModifierOrderBy"], Is.EqualTo(2));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfFilters"], Is.EqualTo(1));
        });
    }

    [Test]
    public void GroupByTest()
    {
        var stats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("GroupBy.sparql"));
        Assert.Multiple(() =>
        {
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfModifierGroupBy"], Is.EqualTo(1));
            Assert.That(stats["http://purl.com/qado/ontology.ttl#numberOfModifierHaving"], Is.EqualTo(1));
        });
    }

    [Test]
    public void PropertyPathTest()
    {
        var alternativeStats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("Alternative.sparql"));
        var pathStats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("Path.sparql"));
        
        Assert.Multiple(() =>
        {
            Assert.That(alternativeStats["http://purl.com/qado/ontology.ttl#numberOfResourcesPredicates"], Is.EqualTo(3));
            Assert.That(pathStats["http://purl.com/qado/ontology.ttl#numberOfResourcesPredicates"], Is.EqualTo(2));
        });
    }

    [Test]
    public void ValuesTest()
    {
        var valuesStats = SparqlParser.AnalyseQuery(_queryParser.ParseFromFile("ValuesQuery.sparql"));
        Assert.That(valuesStats["http://purl.com/qado/ontology.ttl#numberOfResources"], Is.EqualTo(4));
    }
}
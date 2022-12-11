namespace SPARQLParser;

public class SparqlAnalysisStatistics
{
    public List<string> Exceptions { get; } = new();
    public SparqlAnalysisState State { get; set; }
    public int Parsable { get; set; } = 0;
    public int NonParsable { get; set; } = 0;
}
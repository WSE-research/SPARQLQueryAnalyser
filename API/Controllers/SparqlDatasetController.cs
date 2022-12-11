using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using SPARQLParser;
using Docker.DotNet;
using Docker.DotNet.Models;

namespace API.Controllers;

[ApiController]
[Route("/sparql/analyse")]
public class SparqlDatasetController : Controller
{
    private readonly ILogger<SparqlDatasetController> _logger;

    public SparqlDatasetController(ILogger<SparqlDatasetController> logger)
    {
        _logger = logger;
    }
    
    /// <summary>
    /// Get all IDs of build analysis tasks
    /// </summary>
    /// <returns>Array of analysis IDs</returns>
    [HttpGet]
    public IEnumerable<string> GetIds()
    {
        _logger.Log(LogLevel.Information, "Reading analysis IDs");
        var directories = Directory.EnumerateDirectories("analysis");

        return directories.Select(directory => directory.Split(Path.DirectorySeparatorChar).Last()).ToList();
    }

    [HttpGet("{id}")]
    public ActionResult<SparqlAnalysisStatistics> GetStatisticsById(string id)
    {
        var path = Path.Join("analysis", id);

        if (!Path.Exists(path))
        {
            return NotFound();
        }
        
        var foundStatistics = JsonSerializer.Deserialize<SparqlAnalysisStatistics>(System.IO.File.ReadAllText(Path.Join(path, "statistics.json")));

        if (foundStatistics is null)
        {
            return NotFound();
        }
        
        return foundStatistics;
    }

    [HttpPost("db")]
    public ActionResult<SparqlAnalysisState> StartAnalysis(DatabaseConfig config)
    {
        var createdState = new SparqlAnalysisState();
        var runDirectory = BuildDirectory(createdState);
        
        IDbConnector connector;

        switch (config.Type)
        {
            case DbType.Stardog:
                connector = new StardogSparqlConnector($"{config.DatabaseUri}:{config.DatabasePort}", 
                    config.Username, config.Password, config.DatabaseName, config.SelectQuery, config.Upload);
                break;
            case DbType.Unknown:
                return BadRequest("No database type defined");
            default:
                return BadRequest("Missing database type selection");
        }

        _logger.Log(LogLevel.Information, "Start new analysis for {Id} on stardog at {CreatedAt}", createdState.Id, createdState.CreatedAt);

        var queries = connector.GetQueries();

        Directory.CreateDirectory(runDirectory);

        StoreState(queries, createdState, config);
        StartDockerAnalyser(createdState.Id);

        return createdState;
    }
    
    [HttpPost("plaintext")]
    public ActionResult<SparqlAnalysisState> StartAnalysis(PlainTextAnalysisConfig config)
    {
        var createdState = new SparqlAnalysisState();
        var runDirectory = BuildDirectory(createdState);

        _logger.Log(LogLevel.Information, "Start new analysis for {Id} on plain-text at {CreatedAt}", createdState.Id, createdState.CreatedAt);
        
        Directory.CreateDirectory(runDirectory);
        
        StoreState(config.Queries, createdState);
        StartDockerAnalyser(createdState.Id);

        return createdState;
    }

    private static string BuildDirectory(SparqlAnalysisState state)
    {
        return Path.Join("analysis", state.Id);
    }

    private static void StoreState(IEnumerable<string> queries, SparqlAnalysisState state, DatabaseConfig? config = null)
    {
        var directory = BuildDirectory(state);

        var queryString = JsonSerializer.Serialize(queries);
        System.IO.File.WriteAllText(Path.Join(directory, "queries.json"), queryString);

        var connectorString = JsonSerializer.Serialize(config);
        System.IO.File.WriteAllText(Path.Join(directory, "connector.json"), connectorString);

        var stateString = JsonSerializer.Serialize(state);
        System.IO.File.WriteAllText(Path.Join(directory, "state.json"), stateString);
    }

    /// <summary>
    /// Start a Docker container to parse stored queries
    /// </summary>
    /// <param name="id">ID of the task to be analysed</param>
    private async void StartDockerAnalyser(string id)
    {
        _logger.Log(LogLevel.Information, "Start parsing of {Id}...", id);
        var client = new DockerClientConfiguration().CreateClient();

        var fullIdPath = Path.GetFullPath(Path.Join("analysis", id));

        var containerConfig = await client.Containers.CreateContainerAsync(new CreateContainerParameters()
        {
            Image = "sparql-analyser:latest",
            HostConfig = new HostConfig
            {
                Mounts = new List<Mount> { new() {Source = fullIdPath, Target = "/analyse", Type = "bind"}}
            }
        });

        var containerId = containerConfig.ID;

        if (containerId is null) return;
        
        _logger.Log(LogLevel.Information, "Starting container {Id}", containerId);
        await client.Containers.StartContainerAsync(containerId, new ContainerStartParameters());
    }
}
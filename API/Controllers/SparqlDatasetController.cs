using System.ComponentModel;
using System.Text.Json;
using AngleSharp.Common;
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

    /// <summary>
    /// Get the output of the current statistics
    /// </summary>
    /// <param name="id">ID of the task</param>
    /// <returns>Statistics of the parsed SPARQL queries</returns>
    [HttpGet("{id}")]
    public ActionResult<SparqlAnalysisStatistics> GetStatisticsById(string id)
    {
        var path = Path.Join("analysis", id);

        if (!Path.Exists(path))
        {
            return NotFound();
        }

        try
        {
            var statistics = JsonSerializer.Deserialize<SparqlAnalysisStatistics>(
                System.IO.File.ReadAllText(Path.Join(path, "statistics.json")));

            if (statistics is null)
                return NotFound();

            return statistics;
        }
        catch (FileNotFoundException)
        {
            return NotFound();
        }
    }

    /// <summary>
    /// Creates an asynchronous task for SPARQL analysis using a remote DB
    /// </summary>
    /// <param name="config">Configuration for the task</param>
    /// <returns>Initialized state with task id</returns>
    [HttpPost("db")]
    public ActionResult<SparqlAnalysisState> StartAnalysis(DatabaseConfig config)
    {
        var createdState = new SparqlAnalysisState();
        var runDirectory = BuildDirectory(createdState);

        try
        {
            var connector = config.Construct();

            _logger.Log(LogLevel.Information, "Start new analysis for {Id} on stardog at {CreatedAt}", createdState.Id,
                createdState.CreatedAt);

            var queries = connector.GetQueries();

            Directory.CreateDirectory(runDirectory);

            StoreState(queries, createdState, config);
            StartDockerAnalyser(createdState.Id);

            return createdState;
        }
        catch (InvalidEnumArgumentException)
        {
            return BadRequest("Invalid DB type provided!");
        }
    }
    
    /// <summary>
    /// Creates an asynchronous task for SPARQL analysis using a list of queries
    /// </summary>
    /// <param name="config">Configuration for the task</param>
    /// <returns>Initialized state with task id</returns>
    [HttpPost("plaintext")]
    public ActionResult<SparqlAnalysisState> StartAnalysis(PlainTextAnalysisConfig config)
    {
        var createdState = new SparqlAnalysisState();
        var runDirectory = BuildDirectory(createdState);

        _logger.Log(LogLevel.Information, "Start new analysis for {Id} on plain-text at {CreatedAt}", createdState.Id, createdState.CreatedAt);
        
        Directory.CreateDirectory(runDirectory);

        var queriesWithId = new List<string>();

        for (var i = 0; i < config.Queries.Count(); i++)
        {
            queriesWithId.Add($@"urn:db-0-question-{i}/||\urn:db-0-query-{i}/||\{config.Queries.GetItemByIndex(i)}");
        }
        
        StoreState(queriesWithId, createdState);
        StartDockerAnalyser(createdState.Id);

        return createdState;
    }

    /// <summary>
    /// Get the directory for a given task
    /// </summary>
    /// <param name="state">State of the task</param>
    /// <returns>Local directory path for the given task</returns>
    private static string BuildDirectory(SparqlAnalysisState state)
    {
        return Path.Join("analysis", state.Id);
    }

    /// <summary>
    /// Serializes all data needed for the analyser task
    /// </summary>
    /// <param name="queries">List of SPARQL queries</param>
    /// <param name="state">Current state of the task</param>
    /// <param name="config">Configuration of DB connection if used</param>
    private static void StoreState(IEnumerable<string> queries, SparqlAnalysisState state, DatabaseConfig? config = null)
    {
        var directory = BuildDirectory(state);

        System.IO.File.WriteAllLines(Path.Join(directory, "queries"), queries);

        System.IO.File.WriteAllText(Path.Join(directory, "connector.json"), JsonSerializer.Serialize(config));

        System.IO.File.WriteAllText(Path.Join(directory, "state.json"), JsonSerializer.Serialize(state));
        
        System.IO.File.WriteAllText(Path.Join(directory, "statistics.json"), JsonSerializer.Serialize(new SparqlAnalysisStatistics
        {
            State = state
        }));
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

        var containerConfig = await client.Containers.CreateContainerAsync(new CreateContainerParameters
        {
            Image = "sparql-analyser:latest",
            HostConfig = GetHostConfig(fullIdPath),
            Env = RunsOnDocker() ? new List<string> { $"docker-analysis-path=analysis/{id}" } : new List<string>()
        });

        var containerId = containerConfig.ID;

        if (containerId is null) return;
        
        _logger.Log(LogLevel.Information, "Starting container {Id}", containerId);
        var started = await client.Containers.StartContainerAsync(containerId, new ContainerStartParameters());

        if (!started)
        {
            _logger.Log(LogLevel.Error, "Container {Id} didn't start!", containerId);
        }
    }

    private static HostConfig GetHostConfig(string fullIdPath)
    {
        var isDocker = RunsOnDocker();

        if (isDocker)
        {
            return new HostConfig
            {
                Binds = new List<string> { "sparql-analyse:/app/analysis" }
            };
        }
        
        return new HostConfig
        {
            Mounts = new List<Mount> { new() { Source = fullIdPath, Target = "/app/analysis", Type = "bind" } }
        };
    }

    private static bool RunsOnDocker()
    {
        return Environment.GetEnvironmentVariable("useDocker") is not null;
    }
}
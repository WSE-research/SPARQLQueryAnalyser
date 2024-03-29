using Docker.DotNet;
using Docker.DotNet.Models;

if (!Directory.Exists("analysis"))
{
    Directory.CreateDirectory("analysis");
}

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

var dockerClient = new DockerClientConfiguration().CreateClient();
await dockerClient.Images.CreateImageAsync(new ImagesCreateParameters
{
    FromImage = "wseresearch/sparql-analyser",
    Tag = "latest"
}, null, new Progress<JSONMessage>());

await dockerClient.Images.TagImageAsync("wseresearch/sparql-analyser:latest", new ImageTagParameters
{
    RepositoryName = "sparql-analyser",
    Tag = "latest"
});

app.Run();
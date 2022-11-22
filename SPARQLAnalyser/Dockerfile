FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["SPARQLAnalyser/SPARQLAnalyser.csproj", "SPARQLAnalyser/"]
RUN dotnet restore "SPARQLAnalyser/SPARQLAnalyser.csproj"
COPY . .
WORKDIR "/src/SPARQLAnalyser"
RUN dotnet build "SPARQLAnalyser.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "SPARQLAnalyser.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SPARQLAnalyser.dll"]

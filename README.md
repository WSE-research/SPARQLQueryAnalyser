# QADO SPARQL Query Analyser
This service provides a Web API to process the SPARQL queries of the
QADO dataset and enriches them with additional properties. It contains
2 components:
* `SPARQL Query Analyser Web API`: Web API for starting SPARQL query analyses
* `SPARQL Analyser`: asynchronous execution of analysis tasks

## Usage
The service is available as a pre-build Docker Image and can be run
by executing the following commands:

```shell
docker pull wseresearch/sparql-analyser-api:latest
docker run -itd -v /var/run/docker.sock:/var/run/docker.sock -p 8080:80 wseresearch/sparql-analyser-api:latest
```

The API description is available at http://localhost:8080/swagger/index.html.
For using the service a triplestore with the [QADO dataset](https://github.com/WSE-research/QADO-datasets)
is required. Currently, [Stardog](https://www.stardog.com/) and [GraphDB](https://www.ontotext.com/products/graphdb/) are supported.

The service generates the following properties that can be uploaded to 
the triplestore automatically:
* `qado:Query`
  * `qado:numberOfModifierOrderBy`
  * `qado:numberOfModifierLimit`
  * `qado:numberOfModifierHaving`
  * `qado:numberOfModifierOffset`
  * `qado:numberOfModifierGroupBy`
  * `qado:numberOfModifiers`
  * `qado:numberOfTriples`
  * `qado:numberOfFilters`
  * `qado:numberOfVariables`
  * `qado:numberOfResources`
  * `qado:numberOfResourcesSubjectsObjects`
  * `qado:numberOfResourcesPredicates`
  * `qado:normalizedQueryLength`
* `qado:Dataset`
  * `qado:hasPrefix`

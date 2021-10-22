# Wikidata Benchmark

In this repo you can find a set of data files and queries to Wikidata.

## Wikidata data

The data we store/share is Wikidata version date

* [Wikidata Graph Pattern Benchmark (WGPB) for RDF/SPARQL](https://zenodo.org/record/4035223). The Wikidata Graph Pattern Benchmark (WGPB) is a benchmark using Wikidata truthy graph with English labels filtering triples with rare (<1000 triples) and very common (>1000000) predicates. The Wikidata graph was downloaded on XXX.
* [MilleniumDB data](https://drive.google.com/file/d/1oDkrHT68_v7wfzTxjaRg40F7itb7tVEZ/view?usp=sharing)

## Wikidata Queries

The Wikidata queries come from two sources, the WGPB benchmark queries and the MilleniumDB benchmark queries. All queries from both query sets  are guaranteed to return at least one result. 

### WGPB Benchmark queries

The following queries contain at leats one Basic Graph Pattern (BGP), zero or more `OPTIONAL` and zero or more *Property Path* expressions. These queries were extracted using the following [script](https://github.com/aidhog/wikidata-benchmark/), in which there are classes to extract BGPs (ParseBGPsARQ), Optionals (ParseOptsARQ), Property paths (ParsePathsARQ), and queries returning numeric results (ParseNumericQueriesARQ).

The Wikidata query log files can be found [here](https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en).


Para "normalizar" un poco el benchmark, sería bueno:
- usar siempre los mismos archivos de log de Wikidata como base 
- aplicar el mismo proceso para filtrar consultas
- pensar en usar LIMIT, DISTINCT, SELECT * o con variables, etc.
- definir los datos RDF usados

También creo que sería bueno incluir otros tipos de consulta, en particular:
- BGPs con property paths

¿Te parece trabajar en esas cosas?

### MilleniumDB Benchmark queries
* [Carlos Rojas queries](queries/). These queries are path queries have both a fixed start and end node. That is, there is a graph node before the path expresion and at the end of the expression. 
  - tienen punto fijo de partida o llegada
  - En el property path solo se usan properties IRIs del estilo: http://www.wikidata.org/prop/direct/P*

### Differences betwee both sets of queries

 * one has `OPTIONAL` while the other does not.
 * one is SPARQL, the other MilleniumDBQL
 * one is for a generic system, the other for MilleniumDB
 
 ## Benchmark goal
 
This is a stress test for a Wikidata storage system. What that system being test is able to support and what is not. It contains the scripts to run the bechmarks, the data and query cleaning.


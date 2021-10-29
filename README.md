# Wikidata Benchmark

In this repository you can find the data files and queries to Wikidata used in the evaluation section for the publication [MillenniumDB: A Modular Architecture for Persistent Graph Database Systems](https://ahivx.org). These data and queries are the input for the script files that run the evaluation.

## Wikidata data

The data used in this evaluation are the [Wikidata Truthy](https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en) from 2021-06-23. We cleaned the data removing labels from nodes and those properties that do not belong to the Wikidata's vocabulary (i.e `http://www.wikidata.org/prop/direct/P`). The data is available to download from [Figshare](https://figshare).

The scripts to generate these data from the [original data](https://www.wikidata.org/wiki/Wikidata:Database_download) are in our [source folder](src/py/filter_direct_properties.py) folder.

## Wikidata Queries

We obtained thee queries we used in our benchmark from the [Wikidata query log files](https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en). We also filtered those so all queries have at least one result and have a fixed starting or ending node. That is, there is a graph node before a path expression and at the end of that expression. We used the Java classes in our [src folder](src/src) and stored the resulting queries in our [data folder](data/bgps).

From the previous queries we generate our final query set in the different query languages (SPARQL, Cypher and MilleniumDB Query Language) using the Python script [translator_sparql_2_mdb.py](src/py/translator_sparql_2_mdb.py).


## Scripts

Here we provide a description of the scripts and configuration files we used for loading the data into the different engines and the execution scripts for the queries.

### Running scripts

### Data loading scripts

The script [benchmark_bgps.py](src/py/benchmark_bgps.py) runs the evaluation for queries containing only Basic Graph Patterns. 
The input parameters are:
 * The query engine that will run the queries
 * A single file containing all queries, one in each line
 * The result set size limit
 * The prefixes used in the evaluation

The script [benchmark_property_paths.py](src/py/benchmark_property_paths.py) runs the evaluation for queries containing SPARQL property paths. 
The input parameters are:
 * The query engine that will run the queries
 * A single file containing all queries, one in each line
 * The result set size limit

We also provide a Python script that translates SPARQL queries to the MilleniumDB syntax in [translator_sparql_2_mdb.py](src/py/translator_sparql_2_mdb.py)

 


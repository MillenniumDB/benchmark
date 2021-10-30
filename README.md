# Wikidata Benchmark

In this repository you can find the data files and queries to Wikidata used in the evaluation section for the publication [MillenniumDB: A Modular Architecture for Persistent Graph Database Systems](https://ahivx.org). These data and queries are the input for the script files that run the evaluation.

## Wikidata data

The data used in this evaluation are the [Wikidata Truthy](https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en) from 2021-06-23. We cleaned the data removing labels from nodes and those properties that do not belong to the Wikidata's vocabulary (i.e `http://www.wikidata.org/prop/direct/P`). The data is available to download from [Google Drive](https://drive.google.com/u/0/uc\?id\=1oDkrHT68_v7wfzTxjaRg40F7itb7tVEZ).

The scripts to generate these data from the [original data](https://www.wikidata.org/wiki/Wikidata:Database_download) are in our [source folder](src/py/filter_direct_properties.py) folder.

## Wikidata Queries

We obtained thee queries we used in our benchmark from the [Wikidata query log files](https://iccl.inf.tu-dresden.de/web/Wikidata_SPARQL_Logs/en). We also filtered those so all queries have at least one result and have a fixed starting or ending node. That is, there is a graph node before a path expression and at the end of that expression. We used the Java classes in our [src folder](src/src) and stored the resulting queries in our [data folder](data/bgps).

From the previous queries we generate our final query set in the different query languages (SPARQL, Cypher and MilleniumDB Query Language) using the Python script [translator_sparql_2_mdb.py](src/py/translator_sparql_2_mdb.py).


## Scripts

Here we provide a description of the scripts and configuration files we used for loading the data into the different engines and the execution scripts for the queries.

### Running scripts

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

### Data loading scripts for MilleniumDB
 
### Data loading scripts for Jena

#### Prerequisites 

The only prerequisite is Java JDK (we used openjdk 11, other versions might work as well)

The installation may be different depending on your linux distribution. For Debian/Ubuntu based distributions:

- `sudo apt update`
- `sudo apt install openjdk-11-jdk`

#### Download apache jena

You can download Apache Jena from their [website](https://jena.apache.org/download/) . The file you need to download will look like `apache-jena-4.X.Y.tar.gz`, in our case, we used the version `4.1.0`, but this should also work for newer versions.

#### Extract and change into the project folder

- `tar -xf apache-jena-4.*.*.tar.gz`
- `cd apache-jena-4.*.*/`

#### Execute the bulk import

- `bin/tdbloader2 --loc=[path_of_new_db_folder] [path_of_nt_file]`

#### Import for leapfrog version

This step is necessary only if you want to use the leapfrog jena implementation, you can skip this otherwise.

Edit the text file `bin/tdbloader2index` and search for the lines:

```
generate_index "$K1 $K2 $K3" "$DATA_TRIPLES" SPO

generate_index "$K2 $K3 $K1" "$DATA_TRIPLES" POS

generate_index "$K3 $K1 $K2" "$DATA_TRIPLES" OSP
```

After those lines add:

```
generate_index "$K1 $K3 $K2" "$DATA_TRIPLES" SOP

generate_index "$K2 $K1 $K3" "$DATA_TRIPLES" PSO

generate_index "$K3 $K2 $K1" "$DATA_TRIPLES" OPS
```

Now you can execute the bulk import in the same way we did it before:

- `bin/tdbloader2 --loc=[path_of_new_db_folder] [path_of_nt_file]`

### Virtuoso import instructions

#### 1. Edit the .nt

Virtuoso has a problem with geo-datatypes so we generated a new .nt file to prevent them from being parsed as a geo-datatype.

- `sed 's/#wktLiteral/#wktliteral/g' [path_of_nt_file] > [virtuoso_nt_file]`

#### Download Virtuoso

You can download Virtuoso from their github:
https://github.com/openlink/virtuoso-opensource/releases.
We used Virtuoso Open Source Edition, version 7.2.6.

- Download: `wget https://github.com/openlink/virtuoso-opensource/releases/download/v7.2.6.1/virtuoso-opensource.x86_64-generic_glibc25-linux-gnu.tar.gz`
- Extract: `tar -xf virtuoso-opensource.x86_64-generic_glibc25-linux-gnu.tar.gz`
- Enter to the folder: `cd virtuoso-opensource`

#### Create configuration file

- We start from their example configuration file:

  - `cp database/virtuoso.ini.sample wikidata.ini`

- Edit `wikidata.ini` with any text editor, when you edit a path, we recomend using the absolute path:

  - replace every `../database/` with the path of the database folder you want to create.

  - add the path of folder where you have `[virtuoso_nt_file]` and the path of the database folder you want to create to `DirsAllowed`.

  - change `VADInstallDir` to the path of `virtuoso-opensource/vad`.

  - set `NumberOfBuffers`. For loading the data we used `7200000`, to run experiments we used `5450000`.

  - set `MaxDirtyBuffers`. For loading the data we used `5400000`, to run experiments we used `4000000`.

  - revise `ResultSetMaxRows`, our experiments set this to `1000000`

  - revise `MaxQueryCostEstimationTime`, our experiments commented this out with ';' before the line removing the limit

  - revise `MaxQueryExecutionTime`, our experiments used `600` for 10 minute timeouts

  - add at the end of the file:

    ```
    [Flags]
    tn_max_memory = 2755359740
    ```

#### Load the data

- Start the server: `bin/virtuoso-t -c wikidata.ini +foreground`

  - This process won't end until you interrupt it (Ctrl+C). Let this execute until the import is finished. Run the next command in another terminal.

- `bin/isql localhost:1111`

  And inside the isql console run:

  - `ld_dir('[path_to_virtuoso_folder]', '[virtuoso_nt_file]', 'http://wikidata.org/);`
  - `rdf_loader_run();`

### Blazegraph import instructions

#### Prerequisites

You'll need the following prerequisites installed:

- Java JDK (with `$JAVA_HOME` defined and `$JAVA_HOME/bin` on `$PATH`)
- Maven
- Git

The installation may be different depending on your linux distribution. For Debian/Ubuntu based distributions:

- `sudo apt update`
- `sudo apt install openjdk-11-jdk mvn git`

#### Split .nt file into smaller files

Blazegraph can't load big files in a reasonable time, so we need to split the .nt into smaller files (1M each)

- `mkdir splitted_nt`
- `cd splitted_nt`
- `split -l 1000000 -a 4 -d --additional-suffix=.nt [path_to_nt]`
- `cd ..`

#### Clone repo and build

- `git clone --recurse-submodules https://gerrit.wikimedia.org/r/wikidata/query/rdf wikidata-query-rdf`
- `cd wikidata-query-rdf`
- `mvn package`
- `cd dist/target`
- `tar xvzf service-*-dist.tar.gz`
- `cd service-*/`
- `mkdir logs`

#### Edit the default script

- Edit the script file `runBlazegraph.sh` with any text editor.
  - configure main memory here: `HEAP_SIZE=${HEAP_SIZE:-"64g"}` (You may use other value dependeing on how much RAM your machine has)
  - set the log folder `LOG_DIR=${LOG_DIR:-"/path/to/logs"}`, replace `/path/to/logs` with the absolute path of the `logs` dir you created in the previous step.
  - add `-Dorg.wikidata.query.rdf.tool.rdf.RdfRepository.timeout=600` to the `exec java` command to specify the timeout (value is in seconds).
  - also change `-Dcom.bigdata.rdf.sparql.ast.QueryHints.analyticMaxMemoryPerQuery=0` which removes per-query memory limits.

#### Load the splitted data

- Start the server: `./runBlazegraph.sh`
  - This process won't end until you interrupt it (Ctrl+C). Let this execute until the import is finished. Run the next command in another terminal.
- Start the import: `./loadRestAPI.sh -n wdq -d [path_of_splitted_nt_folder]`

from SPARQLWrapper import SPARQLWrapper, JSON
from socket import timeout
import time
import subprocess
import traceback
import os
import re
import sys

# Usage:
# python benchmark_property_paths.py <ENGINE> <QUERIES_FILE_ABSOLUTE_PATH> <LIMIT> <SUFFIX_NAME>
# LIMIT = 0 will not add a limit

if len(sys.argv) < 5:
    print("Remember to edit the parameters in this script before executing")
    print("usage:\npython benchmark_property_paths.py <ENGINE> <QUERIES_FILE_ABSOLUTE_PATH> <LIMIT> <SUFFIX_NAME>")
    print("example:\npython src/benchmarking/benchmark_property_paths.py MILLENNIUM $(pwd)/queries/sparql_property_paths.txt 1000 v1")
    sys.exit()

# Db engine that will execute queries
ENGINE       = sys.argv[1]
QUERIES_FILE = sys.argv[2]
LIMIT        = sys.argv[3]
SUFFIX_NAME  = sys.argv[4]

############################ EDIT THIS PARAMETERS #############################
TIMEOUT = 600 # Max time per query in seconds

BENCHMARK_ROOT = '/data2/benchmark/'

# MILLENNIUM DB options
MDB_WIKIDATA_PATH =  f'{BENCHMARK_ROOT}/MillenniumDB/tests/dbs/wikidata'
MDB_BUFFER_SIZE = 8388608 # Buffer used by MillenniumDB 8388608 == 32GB
                          # using 32GB instead of 64GB because
                          # this is only for the buffer manager and MillenniumDB
                          # needs more RAM for other things (e.g. ObjectFile)

# Prefer use absolute paths to avoid problems with current directory
ENGINES_PATHS = {'MILLENNIUM': f'{BENCHMARK_ROOT}/MillenniumDB',
                 'JENA':       f'{BENCHMARK_ROOT}/jena',
                 'BLAZEGRAPH': f'{BENCHMARK_ROOT}/blazegraph/service',
                 'VIRTUOSO':   f'{BENCHMARK_ROOT}/virtuoso'}

ENGINES_PORTS = {'MILLENNIUM': 8080,
                 'JENA':       3030,
                 'VIRTUOSO':   1111,
                 'BLAZEGRAPH': 9999}

ENDPOINTS = {'BLAZEGRAPH': 'http://localhost:9999/bigdata/namespace/wdq/sparql',
             'JENA':       'http://localhost:3030/jena/sparql',
             'VIRTUOSO':   'http://localhost:8890/sparql'}

SERVER_CMD = {
    'MILLENNIUM': ['./build/Release/bin/server', MDB_WIKIDATA_PATH, '-b', str(MDB_BUFFER_SIZE), '--timeout', str(TIMEOUT)],
    'BLAZEGRAPH': ['./runBlazegraph.sh'],
    'JENA': f'java -Xmx64g -jar apache-jena-fuseki-4.1.0/fuseki-server.jar --loc=apache-jena-4.1.0/wikidata --timeout={TIMEOUT*1000} /jena'.split(' '),
    'VIRTUOSO': ['bin/virtuoso-t', '-c', 'wikidata.ini', '+foreground']}

PORT = ENGINES_PORTS[ENGINE]

# Path to needed output and input files
MDB_RESULTS_FILE = f'{BENCHMARK_ROOT}/out/.mdb_temp'
RESUME_FILE      = f'{BENCHMARK_ROOT}/out/paths_{ENGINE}_limit_{LIMIT}_{SUFFIX_NAME}.csv'
ERROR_FILE       = f'{BENCHMARK_ROOT}/out/errors/paths_{ENGINE}_limit_{LIMIT}_{SUFFIX_NAME}.log'

SERVER_LOG_FILE  = f'{BENCHMARK_ROOT}/out/log/paths_{ENGINE}_limit_{LIMIT}_{SUFFIX_NAME}.log'

VIRTUOSO_LOCK_FILE = f'{BENCHMARK_ROOT}/virtuoso/wikidata/virtuoso.lck'
######################## END OF EDITABLE PARAMETERS ###########################
MDB_QUERY_FILE = '/data2/benchmark/mdb_query_file'  # File to write the query

# create output folders if they doesn't exist
if not os.path.exists(f'{BENCHMARK_ROOT}/out/log/'):
    os.makedirs(f'{BENCHMARK_ROOT}/out/log/')

if not os.path.exists(f'{BENCHMARK_ROOT}/out/errors/'):
    os.makedirs(f'{BENCHMARK_ROOT}/out/errors/')

server_log = open(SERVER_LOG_FILE, 'w')
server_process = None

# Check if output file already exists
if os.path.exists(RESUME_FILE):
    print(f'File {RESUME_FILE} already exists. Remove it or choose another prefix.')
    sys.exit()

# ================== Auxiliars ===============================
def lsof(pid):
    process = subprocess.Popen(['lsof', '-a', f'-p{pid}', f'-i:{PORT}', '-t'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, _ = process.communicate()
    return out.decode('UTF-8').rstrip()

def lsofany():
    process = subprocess.Popen(['lsof', '-t', f'-i:{PORT}'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, _ = process.communicate()
    return out.decode('UTF-8').rstrip()


# ================== Parsers =================================
def parse_to_sparql(query):
    if not LIMIT:
        return f'SELECT DISTINCT ?x WHERE {{ {query} }}'
    return f'SELECT DISTINCT ?x WHERE {{ {query} }} LIMIT {LIMIT}'


# Does not return, it writes the query in MDB_QUERY_FILE
def parse_to_millenniumdb(query):
    query_parts   = query.strip().split(' ')
    from_string   = query_parts[0]
    property_path = " ".join(query_parts[1:len(query_parts) - 1])
    end_string    = query_parts[len(query_parts) - 1]

    # Parse subject
    if '?x' in from_string:
        from_string = '(?x)=['
    else:
        from_string = '(' + from_string.split('/')[-1].replace('>', '') + ')=['

    # Parse object
    if '?x' in end_string:
        end_string  = ']=>(?x)'
    else:
        end_string  = ']=>(' + end_string.split('/')[-1].replace('>', '') + ')'

    # Parse property path
    pattern = r"\<[a-zA-Z0-9\/\.\:\#]*\>"
    path_edges = re.findall(pattern, property_path)
    clean_property_path = property_path

    for path in path_edges:
        clean_path          = ':' + path.split('/')[-1].replace('>', '')
        clean_property_path = re.sub(path, clean_path, clean_property_path, flags=re.MULTILINE)

    with open(MDB_QUERY_FILE, 'w') as file:
        file.write(f'MATCH {from_string}{clean_property_path}{end_string} RETURN DISTINCT ?x')
        if LIMIT:
            file.write(f' LIMIT {LIMIT}')


def start_server():
    global server_process
    os.chdir(ENGINES_PATHS[ENGINE])
    print('starting server...')

    server_log.write("[start server]\n")
    server_process = subprocess.Popen(SERVER_CMD[ENGINE], stdout=server_log, stderr=server_log)
    print(f'pid: {server_process.pid}')

    # Sleep to wait server start
    while not lsof(server_process.pid):
        time.sleep(1)

    print(f'done')


def kill_server():
    global server_process
    print(f'killing server[{server_process.pid}]...')
    server_log.write("[kill server]\n")
    if ENGINE == 'VIRTUOSO':
        kill_process = subprocess.Popen([f'{ENGINES_PATHS[ENGINE]}/bin/isql', f'localhost:{PORT}', '-K'])
        kill_process.wait()
    else:
        server_process.kill()
        server_process.wait()

    while lsof(server_process.pid):
        time.sleep(1)

    if ENGINE == 'VIRTUOSO':
        kill_process = subprocess.Popen(['rm', VIRTUOSO_LOCK_FILE], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        kill_process.wait()
    print('done')


# Send query to server
def execute_queries():
    with open(QUERIES_FILE) as queries_file:
        for line in queries_file:
            query_number, query = line.split(',')
            print(f'Executing query {query_number}')

            if ENGINE == 'MILLENNIUM':
                query_millennium(query, query_number)
            else:
                query_sparql(query, query_number)


def query_sparql(query_pattern, query_number):
    query = parse_to_sparql(query_pattern)
    count = 0
    sparql_wrapper = SPARQLWrapper(ENDPOINTS[ENGINE])
    sparql_wrapper.setTimeout(TIMEOUT+10)
    sparql_wrapper.setReturnFormat(JSON)
    sparql_wrapper.setQuery(query)

    start_time = time.time()

    try:
        # Compute query
        results = sparql_wrapper.query()
        json_results = results.convert()
        for _ in json_results["results"]["bindings"]:
            count += 1

        elapsed_time = int((time.time() - start_time) * 1000) # Truncate to miliseconds

        with open(RESUME_FILE, 'a') as file:
            file.write(f'{query_number},{count},OK,{elapsed_time}\n')

    except timeout as e:
        elapsed_time = int((time.time() - start_time) * 1000) # Truncate to miliseconds
        with open(RESUME_FILE, 'a') as file:
            file.write(f'{query_number},{count},TIMEOUT,{elapsed_time}\n')

        with open(ERROR_FILE, 'a') as file:
            file.write(f'Exception in query {str(query_number)}: {str(e)}\n')

        kill_server()
        start_server()

    except Exception as e:
        elapsed_time = int((time.time() - start_time) * 1000) # Truncate to miliseconds
        with open(RESUME_FILE, 'a') as file:
            file.write(f'{query_number},{count},ERROR({type(e).__name__}),{elapsed_time}\n')

        with open(ERROR_FILE, 'a') as file:
            file.write(f'Exception in query {str(query_number)} [{type(e).__name__}]: {str(e)}\n')

        kill_server()
        start_server()



def query_millennium(query, query_number):
    parse_to_millenniumdb(query)
    start_time = time.time()
    with open(MDB_RESULTS_FILE, 'w') as results_file, open(MDB_QUERY_FILE, 'r') as query_file:
        query_execution = subprocess.Popen(
            ['./build/Release/bin/query'],
            stdin=query_file,
            stdout=results_file,
            stderr=subprocess.DEVNULL)
    exit_code = query_execution.wait()
    elapsed_time = int((time.time() - start_time) * 1000)
    p = subprocess.Popen(['wc', '-l', MDB_RESULTS_FILE], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, _ = p.communicate()
    results_count = int(result.strip().split()[0]) - 6 # 1 line from header + 2 for separators + 3 for stats

    with open(RESUME_FILE, 'a') as file:
        if exit_code == 0:
            file.write(f'{query_number},{results_count},OK,{elapsed_time}\n')
        else:
            if elapsed_time >= TIMEOUT:
                file.write(f'{query_number},{results_count},TIMEOUT,{elapsed_time}\n')
            else:
                file.write(f'{query_number},0,ERROR,{elapsed_time}\n')


with open(RESUME_FILE, 'w') as file:
    file.write('query_number,results,status,time\n')

with open(ERROR_FILE, 'w') as file:
    file.write('') # to replaces the old error file

if lsofany():
    raise Exception("other server already running")

start_server()
execute_queries()

# Delete temp file
subprocess.Popen(['rm', MDB_RESULTS_FILE], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
subprocess.Popen(['rm', MDB_QUERY_FILE], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if server_process is not None:
    kill_server()

server_log.close()
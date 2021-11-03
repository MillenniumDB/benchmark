from neo4j import GraphDatabase
import time
import sys
import os

QUERIES_FILE = sys.argv[1]
LIMIT        = sys.argv[2]

############################ EDIT THIS PARAMETERS #############################
BENCHMARK_ROOT = '/data2/benchmark'
RESUME_FILE = f'{BENCHMARK_ROOT}/results/property_paths_NEO4J_limit_{LIMIT}.txt'

BOLT_ADDRESS = 'bolt://127.0.0.1:7687'
DATABASE_NAME = 'wikidata'
###############################################################################

driver = GraphDatabase.driver(BOLT_ADDRESS)

with driver.session(database=DATABASE_NAME) as session, open(QUERIES_FILE, 'r') as queries_file:
    for line in queries_file:
        query_number, query = line.strip().split(',')
        result_count = 0
        start_time = time.time()
        try:
            print("executing query", query_number)
            if LIMIT != 0:
                query += f' LIMIT {LIMIT}'
            results = session.read_transaction(lambda tx: tx.run(query).data())
            for _ in results:
                result_count += 1

            elapsed_time = int((time.time() - start_time) * 1000) # Truncate to miliseconds
            with open(RESUME_FILE, 'a') as output_file:
                output_file.write(f'{query_number},{result_count},OK,{elapsed_time}\n')

        except Exception as e:
            elapsed_time = int((time.time() - start_time) * 1000) # Truncate to miliseconds
            with open(RESUME_FILE, 'a') as output_file:
                output_file.write(f'{query_number},{result_count},ERROR({type(e).__name__}),{elapsed_time}\n')
            print(e)
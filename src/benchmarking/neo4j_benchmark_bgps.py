from neo4j import GraphDatabase
import time
import sys
import os
import re

QUERIES_FILE = sys.argv[1]
LIMIT        = sys.argv[2]

############################ EDIT THIS PARAMETERS #############################
BENCHMARK_ROOT = '/data2/benchmark'
RESUME_FILE = f'{BENCHMARK_ROOT}/results/bgps_NEO4J_limit_{LIMIT}.csv'

BOLT_ADDRESS = 'bolt://127.0.0.1:7687'
DATABASE_NAME = 'wikidata'
###############################################################################

# Check if output file already exists
if os.path.exists(RESUME_FILE):
    print(f'File {RESUME_FILE} already exists.')
    sys.exit()
else:
    with open(RESUME_FILE, 'w', encoding='UTF-8') as file:
        file.write('query_number,results,status,time\n')


def IRI_to_neo(iri):
    entity_expressions = []
    expressions = []

    # property
    entity_expressions.append(re.compile(r"^<http://www\.wikidata\.org/prop/direct/([QqPp]\d+)>$"))

    # entity
    entity_expressions.append(re.compile(r"^<http://www\.wikidata\.org/entity/([QqPp]\d+)>$"))

    for expression in entity_expressions:
        match_iri = expression.match(iri)
        if match_iri is not None:
            return match_iri.groups()[0], True

    # string
    expressions.append(re.compile(r'^"((?:[^"\\]|\\.)*)"$'))

    # something with schema
    expressions.append(re.compile(r'^"((?:[^"\\]|\\.)*)"\^\^<http://www\.w3\.org/2001/XMLSchema#\w+>$'))

    # string with idiom
    expressions.append(re.compile(r'^"((?:[^"\\]|\\.)*)"@(.+)$'))

    # other url
    expressions.append(re.compile(r"^<(.+)>$"))

    for expression in expressions:
        match_iri = expression.match(iri)
        if match_iri is not None:
            return match_iri.groups()[0], False

    raise Exception(f'unhandled iri: {iri}')


def execute_query(session, query, query_number):
    # print(f'Executing query {query_number}')
    # Remove the ' .' at the end of the string
    query = query.strip()[:-2]

    # Split into triples:
    triples = query.split(' . ')

    mdb_basic_patterns = []
    variables = set()
    for triple in triples:
        s, p, o = triple.split(' ')

        if s[0] == '?':
            s = s[1:]
            variables.add(s + '.id')
        else:
            id, is_entity = IRI_to_neo(s)
            if is_entity:
                s = f':Entity {{id:"{id}"}}'
            else:
                s = f'{{id:"{id}"}}'

        if p[0] == '?':
            p = p[1:]
            variables.add(p + '.id')
        else:
            id, is_entity = IRI_to_neo(p)
            if not is_entity:
                raise Exception(f'Bad predicate: {p}')
            p = f':{id}'

        if o[0] == '?':
            o = o[1:]
            variables.add(o + '.id')
        else:
            id, is_entity = IRI_to_neo(o)
            if is_entity:
                o = f':Entity {{id:"{id}"}}'
            else:
                o = f'{{id:"{id}"}}'

        mdb_basic_patterns.append(f'({s})-[{p}]->({o})')

    match_pattern = ','.join(mdb_basic_patterns)
    select_variables = ','.join(variables)
    cypher_query = f'MATCH {match_pattern} RETURN DISTINCT { select_variables }'
    if LIMIT:
        cypher_query += f' LIMIT {LIMIT}'

    result_count = 0
    start_time = time.time()
    try:
        print("executing query", query_number)
        results = session.read_transaction(lambda tx: tx.run(cypher_query).data())
        for _ in results:
            result_count += 1
        elapsed_time = int((time.time() - start_time) * 1000) # Truncate to miliseconds
        with open(RESUME_FILE, 'a', encoding='UTF-8') as output_file:
            output_file.write(f'{query_number},{result_count},OK,{elapsed_time}\n')

    except Exception as e:
        elapsed_time = int((time.time() - start_time) * 1000) # Truncate to miliseconds
        with open(RESUME_FILE, 'a', encoding='UTF-8') as output_file:
            output_file.write(f'{query_number},{result_count},ERROR({type(e).__name__}),{elapsed_time}\n')
        print(e)



with open(QUERIES_FILE, 'r', encoding='UTF-8') as queries_file:
    driver = GraphDatabase.driver(BOLT_ADDRESS)
    with driver.session(database=DATABASE_NAME) as session:
        for line in queries_file:
            query_number, query = line.split(',')
            execute_query(session, query, query_number)
    driver.close()
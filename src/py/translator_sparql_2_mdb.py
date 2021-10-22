import csv
import re
import pandas as pd

def translation_spartql(query, mode):
    new_query = ''
    ids = {}
    condition = ''
    if mode == 1:
        new_query = to_spartql(query)
    elif mode == 2:
        new_query = to_milenium_db(query)
    return new_query

def delete_row_empty(list_ids, list_query):
    for i,id in enumerate(list_ids):
        list_query.pop(id-i)

    return list_query

def generate_ids_condition_and_ids_empty(condition, list_query):
    ids_condition = []
    ids_empty = []
    in_condition = False
    for id, row in enumerate(list_query):
        if len(row) > 0:
            if condition in row:
                ids_condition.append(id)
                in_condition = True
            elif row[-1] == ',' and in_condition:
                ids_condition.append(id)
            elif in_condition:
                ids_condition.append(id)
                in_condition = False
        else:
            ids_empty.append(id)

    return {
        'ids_condition': ids_condition,
        'ids_empty': ids_empty,
    }

def generate_str_query(list_query):
    new_query = ''

    for str_row in list_query:
        row = f"{str_row}\n"
        new_query = new_query + row

    return new_query


def delete_element_null(list_element):
    new_list = []
    for element in list_element:
        if element:
            new_list.append(element)
    return new_list

def to_milenium_db(query):
    condition = 'MATCH'
    new_query = re.sub('WHERE', 'MATCH', query)
    new_query = new_query.split('\n')
    ids = get_id_condition_and_empty_milenium(list_query=new_query)
    new_query = translation_condition_to_milenium(ids_conditions=ids['ids_condition'], list_query=new_query)
    new_query = delete_row_empty(list_ids=ids['ids_empty'], list_query=new_query)
    new_query = generate_str_query(list_query=new_query)
    return new_query

def get_id_condition_and_empty_milenium(list_query):
    ids_condition = []
    ids_empty = []
    in_condition = False
    condition = 'MATCH'
    for id, row in enumerate(list_query):
        if len(row) > 0:
            str_row  = row.rstrip()
            if condition in str_row:
                ids_condition.append(id)
                in_condition = True
            elif str_row[-1] == '.' and in_condition:
                ids_condition.append(id)
            elif '}' in str_row or '{' in str_row:
                ids_condition.append(id)
            elif in_condition:
                ids_condition.append(id)
                in_condition = False
        else:
            ids_empty.append(id)

    return {
        'ids_condition': ids_condition,
        'ids_empty': ids_empty,
    }

def translation_condition_to_milenium(ids_conditions, list_query):
    condition = 'MATCH'
    condition_delete = '{'
    condition_delete_finish = '}'
    for ids in ids_conditions:
        if condition in list_query[ids] and condition_delete in list_query[ids]:
            list_query[ids] = 'MATCH'
        elif condition_delete == list_query[ids] or condition_delete_finish == list_query[ids]:
            list_query[ids] = ''
        elif condition_delete in list_query[ids]:
            list_query[ids] = re.sub('{', '', list_query[ids])
        elif condition_delete_finish in list_query[ids]:
            list_query[ids] = re.sub('}', '', list_query[ids])
        elif condition not in list_query[ids]:
            new_string = ''
            new_list_query = list_query[ids].split(' ')
            new_list_query = delete_element_null(new_list_query)
            for id, row in enumerate(new_list_query):
                if id == 0:
                    new_string = f'({row})-[:'
                elif id == 1:
                    new_string = f'{new_string}{row}]->('
                else:
                    #print(row)
                    #aux = re.sub('.', '', row)
                    #print(aux)
                    new_string = f'{new_string}{row}),'
            list_query[ids] = new_string
    return list_query

def translation_condition(ids_conditions, list_query):
    list_query[ids_conditions[-1]] = list_query[ids_conditions[-1]] + ',}'
    list_symbol_replace = {
        ']->(': ' ',
        ')': '',
        '),': '',
        '-[:': ' ',
        ',': '.',
        '(': '',
        'WHERE': 'WHERE {'
    }
    for index in ids_conditions:
        for symbol in list_symbol_replace:
            list_query[index] = list_query[index].replace(symbol, list_symbol_replace[symbol])

    return list_query

def to_spartql(query):
    ids = {}
    condition = 'WHERE'
    new_query = re.sub('MATCH', 'WHERE', query)
    new_query = new_query.split('\n')
    ids = generate_ids_condition_and_ids_empty(condition=condition, list_query=new_query)
    new_query = translation_condition(ids_conditions=ids['ids_condition'], list_query=new_query)
    new_query = delete_row_empty(list_ids=ids['ids_empty'], list_query=new_query)
    new_query = generate_str_query(list_query=new_query)

    return new_query


df = pd.read_csv('querys_milenium.csv')
for query in df['querys']:
    spartql_query = translation_spartql(query, 1)
    print(query)
    print(spartql_query)

"""
df = pd.read_csv('querys.csv')
for query in df['querys']:
    milenium_query = translation_spartql(query, 2)
    print(query)
    print(milenium_query)
"""

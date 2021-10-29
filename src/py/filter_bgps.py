import re
import sys

# usage: python [original_bgps] [output_single_filename] [output_multiple_filename]
input_fname           = sys.argv[1]
output_single_fname   = sys.argv[2]
output_multiple_fname = sys.argv[3]

queries = []

with open(input_fname, encoding='utf-8') as file:
    for line in file:
        queries.append(line.strip())

good_single_pattern = []
good_multiple_pattern = []

property_regex = re.compile(r"<http://www\.wikidata\.org/prop/direct/(\D\d+)>")

for query in queries:
    triple_list = query.split(' .')[:-1]
    bad_property = False
    bad_subject  = False
    free_triple  = False # ?s ?p ?o is a free triple

    for triple in triple_list:
        parsed = triple.strip().split(' ')
        s = parsed[0]
        p = parsed[1]
        o = " ".join(parsed[2:])

        if s[0] == '?' and p[0] == '?' and o[0] == '?':
            free_triple = True

        if p[0] != '?' and property_regex.match(p) is None:
            bad_property = True

        if s[0] != '?' and 'http://www.wikidata.org/entity/' not in s:
            bad_subject = True

    if not free_triple and not bad_subject and not bad_property:
        if len(triple_list) == 1:
            good_single_pattern.append(query)
        else:
            good_multiple_pattern.append(query)


with open(output_single_fname, 'w', encoding='utf-8') as output:
    query_number = 0
    for pattern in good_single_pattern:
        query_number += 1
        output.write(f'{query_number},{pattern}\n')

with open(output_multiple_fname, 'w', encoding='utf-8') as output:
    query_number = 0
    for pattern in good_multiple_pattern:
        query_number += 1
        output.write(f'{query_number},{pattern}\n')

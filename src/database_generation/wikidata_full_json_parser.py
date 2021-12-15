# requires python 3.6 or higher
# designed for `wikidata-20201102-all.json` and tested with python 3.8.6
# It might not work with other wikidata versions
# usage :
# python wikidata_full_json_parser.py [input_filename] [output_filename]

import json
import sys
import time
from functools import reduce

anon_counter = 0

def normalize(value):
    if type(value) == str:
        escaped_str = value.replace('\\', '\\\\').replace('"', '\\"')
        return f'\"{escaped_str}\"'

    # TODO: null not supported yet, writing null as string for now
    elif value is None:
        return '\"null\"'

    else:
        return str(value)

# Parse file each line at a time
def parse_lines(file):
    for line in file:
        clean_line = line.strip()
        start = clean_line.find('{')
        if start >= 0:
            end = clean_line.rfind('}') + 1
            yield clean_line[start:end]

# subject is something like 'Q1' if it is an entity or if it is a qualifier '@'
def parse_snak(subject, snak, output_file):
    global anon_counter

    anon_lines = []
    type = snak['property']

    if snak['snaktype'] == 'value':
        value = snak['datavalue']
        value_type = value['type']
        if value_type == 'wikibase-entityid':
            raw_id = value['value']['id']
            object = raw_id.replace('-', '_') # Need to modify ids to be accepted as NamedNodes
            output_file.write(f'{subject}->{object} :{type}\n')

        elif value_type == 'string':
            string_value = normalize(value['value'])
            output_file.write(f'{subject}->{string_value} :{type}\n')

        elif value_type == 'globecoordinate':
            latitude  = normalize(value['value']['latitude'])
            longitude = normalize(value['value']['longitude'])
            precision = normalize(value['value']['precision'])
            globe     = normalize(value['value']['globe'])
            anon_counter += 1
            anon_lines.append(f'_a{anon_counter} :Globecoordinate latitude:{latitude} longitude:{longitude} precision:{precision} globe:{globe}')
            output_file.write(f'{subject}->_a{anon_counter} :{type}\n')

        elif value_type == 'monolingualtext':
            language = normalize(value['value']['language'])
            text     = normalize(value['value']['text'])
            anon_counter += 1
            anon_lines.append(f'_a{anon_counter} :MonolingualText language:{language} text:{text}')
            output_file.write(f'{subject}->_a{anon_counter} :{type}\n')

        elif value_type == 'quantity':
            quantity_properties = [('amount', normalize(value['value']['amount'])),
                                   ('unit',   normalize(value['value']['unit']))]
            if 'upperBound' in value['value']:
                quantity_properties.append(('upperBound', normalize(value['value']['upperBound'])))
            if 'lowerBound' in value['value']:
                quantity_properties.append(('lowerBound', normalize(value['value']['lowerBound'])))

            quantity_properties_str = ''
            for quantity_property in quantity_properties:
                quantity_properties_str += f' {quantity_property[0]}:{quantity_property[1]}'

            anon_counter += 1
            anon_lines.append(f'_a{anon_counter} :Quantity{quantity_properties_str}')
            output_file.write(f'{subject}->_a{anon_counter} :{type}\n')

        elif value_type == 'time':
            time          = normalize(value['value']['time'])
            timezone      = normalize(value['value']['timezone'])
            calendarmodel = normalize(value['value']['calendarmodel'])
            precision     = normalize(value['value']['precision'])

            anon_counter += 1
            anon_lines.append(f'_a{anon_counter} :Time time:{time} timezone:{timezone} calendarmodel:{calendarmodel} precision:{precision}')
            output_file.write(f'{subject}->_a{anon_counter} :{type}\n')

        return anon_lines

    # TODO: do something with novalue or somevalue
    else:
        return None

# Parser
def parse_file(input_filename, output_filename):
    with open(input_filename,  'r', encoding='utf-8') as input_file, \
         open(output_filename, 'w', encoding='utf-8') as output_file:
            for line in parse_lines(input_file):
                data = json.loads(line)

                Entity = data['id']
                # node_label = data['type'] # TODO: add it as a label?

                # Process labels
                label_list = []
                if ('labels' in data) and data['labels']:
                    label_list = data['labels'].values()
                for label in label_list:
                    label_value = normalize(label['value'])
                    label_language = label['language']
                    output_file.write(f'{Entity}->{label_value} :Label language:"{label_language}"\n')

                # Process descriptions
                description_list = []
                if ('descriptions' in data) and data['descriptions']:
                    description_list = data['descriptions'].values()
                for description in description_list:
                    description_value = normalize(description['value'])
                    description_language = description['language']
                    output_file.write(f'{Entity}->{description_value} :Description language:"{description_language}"\n')

                # Process aliases
                alias_list = []
                if ('aliases' in data) and data['aliases']:
                    alias_list = reduce(lambda x, y: x + y, data['aliases'].values())
                for alias in alias_list:
                    alias_value = normalize(alias['value'])
                    alias_language = alias['language']
                    output_file.write(f'{Entity}->{alias_value} :Alias language:"{alias_language}"\n')

                # Process claims
                claim_list = []
                if ('claims' in data) and data['claims']:
                    claim_list = reduce(lambda x, y: x + y, data['claims'].values())
                for claim in claim_list:
                    # TODO: use ranks?
                    # rank_value = None
                    # if 'rank' in claim:
                    #     rank_value = claim['rank']
                    anons = parse_snak(f'{Entity}', claim['mainsnak'], output_file)

                    if anons is not None:
                        if ('qualifiers' in claim) and claim['qualifiers']:
                            for qualifier in reduce(lambda x, y: x + y, claim['qualifiers'].values()):
                                qualifier_anons = parse_snak('@', qualifier, output_file)
                                if qualifier_anons is not None:
                                    anons.extend(qualifier_anons)
                        for after_line in anons:
                            output_file.write(f'{after_line}\n')

                    # TODO: process references?

                # TODO: process sitelinks?


if __name__ == '__main__':
    start = time.time()
    parse_file(sys.argv[1], sys.argv[2])
    end = time.time()
    print('elapsed time:', end - start)


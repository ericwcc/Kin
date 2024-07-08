from pubsub import MessagePublisher
import argparse
import os
import re
import json

KV_OP_PATTERN = re.compile(r'(?P<key>\w+)(?P<op>[=><]=?)(?P<value>.+)')
INDEX_OP_PATTERN = re.compile(r'(?P<key>\w+):(?P<index>\w+)(?P<op>[=><]=?)(?P<value>.+)')

def parse_input_filters(args, input_filters):
    for filter in args.filter:
        kv_op = KV_OP_PATTERN.match(filter)
        if kv_op:
            key = kv_op.group('key')
            if not key in input_filters:
                print(f'error: unknown key {key}')
                continue
            input_filters[key].append((kv_op.group('op'), kv_op.group('value')))
        index_op = INDEX_OP_PATTERN.match(filter)
        if index_op:
            key = index_op.group('key')
            if not key in input_filters:
                print(f'error: unknown key {key}')
                continue
            input_filters[key].append((index_op.group('index'), index_op.group('op'), index_op.group('value')))

def parse_input_mappers(args, input_mappers):
    for mapper in args.mapper:
        kv_op = KV_OP_PATTERN.match(mapper)
        if kv_op:
            input_mappers[kv_op.group('key')] = kv_op.group('value')
        index_op = INDEX_OP_PATTERN.match(mapper)
        if index_op:
            key = index_op.group('key')
            index = index_op.group('index')
            value = index_op.group('value')
            if key == 'column':
                if args.no_header and index.isdigit():
                    input_mappers[f'column_{index}'] = value
                if not args.no_header:
                    input_mappers[f'{index}'] = value
            else:
                print(f'error: unknown key {key}')
                continue

def evaluate_op(operand1, operand2, operator):
    if operator == '>=':
        return operand1 >= operand2
    elif operator == '<=':
        return operand1 <= operand2
    elif operator == '=':
        return operand1 == operand2
    return False

def kafka_error_cb(err):
    print(f'kafka error: {err.str()}')

def publish(args):
    input_filters = {
        'column': [],
        'filename': []
    }

    parse_input_filters(args, input_filters)

    input_mappers = {}

    parse_input_mappers(args, input_mappers)

    publisher = MessagePublisher({'message.timeout.ms': 10,
                                  'socket.timeout.ms': 10,
                                  'error_cb': kafka_error_cb, 
                                  'bootstrap.servers': args.bootstrap_servers})

    try:
        for file in os.listdir(args.directory):
            if not file.endswith('.csv'):
                print(f'error: {file} is not supported')
                continue

            evaluated_filename_filters = [evaluate_op(file, input_value, op) for op, input_value in input_filters['filename']]
            
            if not all(evaluated_filename_filters):
                print(f'filter: file {file} does not match filters {[f"{op}{value}" for i, (op, value) in enumerate(input_filters["filename"]) if not evaluated_filename_filters[i]]}')
                continue

            with open(os.path.join(args.directory, file), 'r') as f:            
                if args.no_header:
                    headers = None
                else:
                    line = f.readline().rstrip()
                    headers = line.split(',')
                
                indexed_column_filters = []
                for index, op, value in input_filters['column']:
                    if headers:
                        if index in headers:
                            index_num = headers.index(index)
                        else:
                            print(f'filter: index not found in headers, index={index}, headers={headers}')
                            continue
                    elif index.isdigit():
                        index_num = int(index)
                    else:
                        print(f'filter: index must be a digit without headers, index={index}')
                        continue
                    
                    indexed_column_filters.append((index_num, op, value))

                for line in f:
                    line = line.rstrip()
                    cells = line.split(',')

                    if headers and len(headers) != len(cells):
                        print(f'error: row must have the same number of cells as the headers, headers={headers}, cells={cells}')
                        continue

                    valued_column_filters = []
                    for i, (index, op, value) in enumerate(indexed_column_filters):
                        if index < len(cells):
                            valued_column_filters.append((cells[index], op, value))
                        else:
                            print(f'error: column index out of bound, filter={input_filters["column"][i]}')
                            continue
                    
                    evaluated_column_filters = [evaluate_op(file_value, input_value, op) for file_value, op, input_value in valued_column_filters]

                    if all(evaluated_column_filters):
                        cell_dict = { headers[i] if headers else f'column_{i}': cell for i, cell in enumerate(cells) }
                        payload_dict = { **cell_dict, 'filename': file[:-4] }
                        mapped_payload_dict = { input_mappers.get(k, k) : v for k, v in payload_dict.items() }
                        mapped_payload_json = json.dumps(mapped_payload_dict).encode('utf-8')

                        success = publisher.produce(topic=args.topic, key='1', value=mapped_payload_json)
                        if not success:
                            print(f'error: failed to publish message, row={line}')
    finally:
        if publisher:
            publisher.flush()
            publisher.close()

def parse_args():
    parser = argparse.ArgumentParser(prog='kin')
    parser.add_argument('-v', '--verbose', action='store_true')

    subparsers = parser.add_subparsers()
    publish_parser = subparsers.add_parser('publish')
    publish_parser.add_argument("directory")
    publish_parser.add_argument('--topic', required=True)
    publish_parser.add_argument('--bootstrap-servers', required=os.getenv('env', 'prod') == 'prod')
    publish_parser.add_argument('--no-header', action='store_true')
    publish_parser.add_argument('--filter', action='append', default=[])
    publish_parser.add_argument('--mapper', action='append', default=[])
    publish_parser.set_defaults(func=publish)
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    args.func(args)
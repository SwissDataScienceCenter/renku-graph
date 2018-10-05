# -*- coding: utf-8 -*-

import argparse
import json
import os


def main():
    parser = argparse.ArgumentParser(description='Transform json LD metadata from renku-python')
    parser.add_argument('input_file', type=str, nargs=1, help='the input file')
    args = parser.parse_args()

    source_file = os.path.realpath(args.input_file[0])
    dir_path = os.path.dirname(source_file)

    with open(source_file, mode='rt') as f:
        json_data = json.load(f)

    projects = extract_projects(json_data)
    print(list(projects))

    persons = extract_persons(json_data)
    print(list(persons))


def extract_projects(json_data):
    filtered = filter(
        lambda obj: 'http://xmlns.com/foaf/0.1/Project' in obj.get('@type', []),
        json_data
    )
    mapped = map(
        lambda obj: {
            'id': obj.get('@id'),
            'url': obj.get('@id'),

        },
        filtered
    )
    return mapped


def extract_persons(json_data):
    filtered = filter(
        lambda obj: 'http://xmlns.com/foaf/0.1/Person' in obj.get('@type', []),
        json_data
    )
    mapped = map(
        lambda obj: {
            'id': obj.get('@id'),
            'email': obj.get('http://xmlns.com/foaf/0.1/mbox', {})[0].get('@id', '').replace('mailto:', ''),
            'name': obj.get('http://xmlns.com/foaf/0.1/name', {})[0].get('@value'),
        },
        filtered
    )
    return mapped


if __name__ == '__main__':
    main()

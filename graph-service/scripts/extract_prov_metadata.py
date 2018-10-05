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

    projects = list(extract_projects(json_data))
    with open(os.path.join(dir_path, 'projects.json'), mode='wt') as f:
        json.dump(projects, f)

    persons = list(extract_persons(json_data))
    with open(os.path.join(dir_path, 'persons.json'), mode='wt') as f:
        json.dump(persons, f)

    entities = list(extract_entities(json_data))
    with open(os.path.join(dir_path, 'entities.json'), mode='wt') as f:
        json.dump(entities, f)

    activities = list(extract_activities(json_data))
    with open(os.path.join(dir_path, 'activities.json'), mode='wt') as f:
        json.dump(activities, f)


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
            'email': obj.get('http://xmlns.com/foaf/0.1/mbox', [{}])[0].get('@id', '').replace('mailto:', ''),
            'name': obj.get('http://xmlns.com/foaf/0.1/name', [{}])[0].get('@value'),
        },
        filtered
    )
    return mapped


def extract_entities(json_data):
    filtered = filter(
        lambda obj: 'http://www.w3.org/ns/prov#Entity' in obj.get('@type', []),
        json_data
    )
    mapped1 = map(
        lambda obj: {
            'id': obj.get('@id'),
            'path_commit': obj.get('http://www.w3.org/2000/01/rdf-schema#label', [{}])[0].get('@value'),
        },
        filtered
    )
    def split_path_commit(obj):
        path, commit = obj.get('path_commit').rsplit('@', 1)
        return {
          'id': obj.get('id'),
          'path': path,
          'commit_sha1': commit,
        }
    return map(split_path_commit, mapped1)


def extract_activities(json_data):
    filtered = filter(
        lambda obj: 'http://www.w3.org/ns/prov#Activity' in obj.get('@type', []),
        json_data
    )
    mapped = map(
        lambda obj: {
            'id': obj.get('@id'),
            'label': obj.get('http://www.w3.org/2000/01/rdf-schema#label', [{}])[0].get('@value'),
            'endTime': obj.get('http://www.w3.org/ns/prov#endedAtTime', [{}])[0].get('@value'),
        },
        filtered
    )
    return mapped


if __name__ == '__main__':
    main()

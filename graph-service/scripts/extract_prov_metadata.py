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

    usages = list(extract_usages(json_data))
    with open(os.path.join(dir_path, 'usages.json'), mode='wt') as f:
        json.dump(usages, f)

    generations = list(extract_generations(json_data))
    with open(os.path.join(dir_path, 'generations.json'), mode='wt') as f:
        json.dump(generations, f)

    associations = list(extract_associations(json_data))
    with open(os.path.join(dir_path, 'associations.json'), mode='wt') as f:
        json.dump(associations, f)


def extract_projects(json_data):
    """Extract project individuals"""
    for obj in json_data:
        if 'http://xmlns.com/foaf/0.1/Project' in obj.get('@type', []):
            yield {
                'id': obj.get('@id'),
                'url': obj.get('@id'),
            }


def extract_persons(json_data):
    """Extract person individuals"""
    for obj in json_data:
        if 'http://xmlns.com/foaf/0.1/Person' in obj.get('@type', []):
            yield {
                'id': obj.get('@id'),
                'email': obj.get('http://xmlns.com/foaf/0.1/mbox', [{}])[0].get('@id', '').replace('mailto:', ''),
                'name': obj.get('http://xmlns.com/foaf/0.1/name', [{}])[0].get('@value'),
            }


def extract_entities(json_data):
    """Extract entities individuals"""
    for obj in json_data:
        if 'http://www.w3.org/ns/prov#Entity' in obj.get('@type', []):
            path_commit = obj.get('http://www.w3.org/2000/01/rdf-schema#label', [{}])[0].get('@value')
            path, commit = path_commit.rsplit('@', 1)
            yield {
                'id': obj.get('@id'),
                'path': path,
                'commit_sha1': commit,
            }


def extract_activities(json_data):
    """Extract activities individuals"""
    for obj in json_data:
        if 'http://www.w3.org/ns/prov#Activity' in obj.get('@type', []):
            yield {
                'id': obj.get('@id'),
                'label': obj.get('http://www.w3.org/2000/01/rdf-schema#label', [{}])[0].get('@value'),
                'endTime': obj.get('http://www.w3.org/ns/prov#endedAtTime', [{}])[0].get('@value'),
            }


def extract_usages(json_data):
    """Extract qualified usage edges"""
    def get_usage_ids():
        for activity in json_data:
            if 'http://www.w3.org/ns/prov#Activity' in activity.get('@type', []):
                for obj in activity.get('http://www.w3.org/ns/prov#qualifiedUsage', []):
                    yield activity.get('@id'), obj.get('@id')

    for activityId, usageId in get_usage_ids():
        usage = next(obj for obj in json_data if obj.get('@id') == usageId)
        yield {
            'activityId': activityId,
            'entityId': usage.get('http://www.w3.org/ns/prov#entity', [{}])[0].get('@id', ''),
        }


def extract_generations(json_data):
    """Extract qualified generation edges"""
    def get_generation_ids():
        for entity in json_data:
            if 'http://www.w3.org/ns/prov#Entity' in entity.get('@type', []):
                for obj in entity.get('http://www.w3.org/ns/prov#qualifiedGeneration', []):
                    yield entity.get('@id'), obj.get('@id')

    for entityId, generationId in get_generation_ids():
        generation = next(obj for obj in json_data if obj.get('@id') == generationId)
        yield {
            'entityId': entityId,
            'activityId': generation.get('http://www.w3.org/ns/prov#activity', [{}])[0].get('@id', ''),
        }


def extract_associations(json_data):
    """Extract qualified association edges"""
    def get_association_ids():
        for activity in json_data:
            if 'http://www.w3.org/ns/prov#Activity' in activity.get('@type', []):
                for obj in activity.get('http://www.w3.org/ns/prov#qualifiedAssociation', []):
                    yield activity.get('@id'), obj.get('@id')

    for activityId, associationId in get_association_ids():
        association = next(obj for obj in json_data if obj.get('@id') == associationId)
        yield {
            'activityId': activityId,
            'agentId': association.get('http://www.w3.org/ns/prov#agent', [{}])[0].get('@id', ''),
            'planId': association.get('http://www.w3.org/ns/prov#hadPlan', [{}])[0].get('@id', ''),
        }


if __name__ == '__main__':
    main()

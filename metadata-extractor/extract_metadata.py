# -*- coding: utf-8 -*-

"""Extracts metadata from a renku project
"""

import argparse
import json
import os
from subprocess import run
from tempfile import TemporaryDirectory

import networkx as nx
from git.objects.commit import Commit
from renku import LocalClient
from renku.cli._graph import Graph
from renku.models.cwl._ascwl import ascwl
from renku.models.cwl.command_line_tool import CommandLineTool
from renku.models.cwl.workflow import Workflow


def main():
    """Main script

    Extracts metadata and outputs it as a json object to stdout
    """
    parser = argparse.ArgumentParser(description='Extracts metadata from a renku project')
    parser.add_argument('clone_url', type=str, nargs='?', default=os.environ.get('RENKU_CLONE_URL'),
                        help='the URL of the target renku project')
    args = parser.parse_args()

    if args.clone_url is None:
        raise ValueError('No clone URL')

    with TemporaryDirectory() as tmp:
        clone_project(args.clone_url, tmp)
        client = LocalClient(os.path.join(tmp, 'renku-project'))
        graph = get_graph(client)
        export_graph = nx.readwrite.json_graph.node_link_data(graph)
        json_graph = json.dumps(export_graph, cls=RenkuEncoder)
        print(json_graph)


def clone_project(clone_url, dest):
    """Clones the given project URL with git"""
    run(['git', 'clone', clone_url, 'renku-project'], cwd=dest).check_returncode()


def get_graph(client):
    """Returns the graph of the project"""
    graph = Graph(client)
    graph.build_status()  # Needed to initialize the graph
    return graph.G


class RenkuEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Commit):
            return {
                'sha1': obj.hexsha,
                'author': '{} <{}>'.format(obj.author.name, obj.author.email),
                'committer': '{} <{}>'.format(obj.committer.name, obj.committer.email),
            }
        if isinstance(obj, LocalClient):
            return '{}'.format(obj)
        if isinstance(obj, CommandLineTool) or isinstance(obj, Workflow):
            return ascwl(obj)
        return super().default(obj)


if __name__ == '__main__':
    main()

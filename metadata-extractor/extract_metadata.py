# -*- coding: utf-8 -*-

"""Extracts metadata from a renku project
"""

import argparse
import os
from subprocess import run
from tempfile import TemporaryDirectory

import networkx as nx
from renku import LocalClient
from renku.cli._graph import Graph


def main():
    """Main script

    Extracts metadata and outputs it as a json object to stdout
    """
    parser = argparse.ArgumentParser(description='Extracts metadata from a renku project')
    parser.add_argument('clone_url', type=str, nargs=1,
                        help='the URL of the target renku project')
    args = parser.parse_args()

    with TemporaryDirectory() as tmp:
        clone_project(args.clone_url[0], tmp)
        client = LocalClient(os.path.join(tmp, 'renku-project'))
        graph = get_graph(client)
        iograph = nx.readwrite.json_graph.node_link_data(graph)
        print(iograph)


def clone_project(clone_url, dest):
    """Clones the given project URL with git"""
    run(['git', 'clone', clone_url, 'renku-project'], cwd=dest).check_returncode()


def get_graph(client):
    """Returns the graph of the project"""
    graph = Graph(client)
    graph.build_status()  # Needed to initialize the graph
    return graph.G


if __name__ == '__main__':
    main()

#!/bin/sh

if [ ! -z "$RENKU_PYTHON_DEV_VERSION" ]
then
    /usr/bin/python3 -m pip uninstall --yes renku
    /usr/bin/python3 -m pip install git+https://github.com/SwissDataScienceCenter/renku-python.git@2271-renku-graph-export
fi

# run the command
$@

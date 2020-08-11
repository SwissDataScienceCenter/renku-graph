#!/bin/sh

if [ ! -z "$RENKU_PYTHON_DEV_VERSION" ]
then
    /usr/bin/python3 -m pip uninstall --yes renku
    /usr/bin/python3 -m pip install ${RENKU_PYTHON_DEV_VERSION}
fi

# run the command
$@

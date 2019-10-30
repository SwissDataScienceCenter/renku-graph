#!/bin/env bash
#
# Copyright 2019 - Swiss Data Science Center (SDSC)
# A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
# Eidgenössische Technische Hochschule Zürich (ETHZ).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

source $(dirname "$0")/travis-functions.sh

# create a release if this build is not triggered by a tag and NOT a new snapshot setting push
COMMIT_MESSAGE_PATTERN="Setting version to .*"
if [[ -z $TRAVIS_TAG ]] && [[ ! $TRAVIS_COMMIT_MESSAGE =~ $COMMIT_MESSAGE_PATTERN ]]; then
  createRelease
  publishCharts
# publish charts if this build is not triggered by a tag and a new snapshot setting push
elif [[ -z $TRAVIS_TAG ]] && [[ $TRAVIS_COMMIT_MESSAGE =~ $COMMIT_MESSAGE_PATTERN ]]; then
  publishCharts
fi

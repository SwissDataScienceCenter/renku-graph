#!/bin/env bash
#
# Copyright 2018 - Swiss Data Science Center (SDSC)
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

make login

# decrypt ssh key to use for docker hub login
openssl aes-256-cbc -K $encrypted_b7eb5d86688a_key -iv $encrypted_b7eb5d86688a_iv -in deploy_rsa.enc -out deploy_rsa -d
chmod 600 deploy_rsa
eval $(ssh-agent -s)
ssh-add deploy_rsa

# fixing git setup
echo "Fixing git setup for $TRAVIS_BRANCH"
git checkout ${TRAVIS_BRANCH}
git branch -u origin/${TRAVIS_BRANCH}
git config branch.${TRAVIS_BRANCH}.remote origin
git config branch.${TRAVIS_BRANCH}.merge refs/heads/${TRAVIS_BRANCH}
git config --global user.name "SwissDataScienceCentre"
git config --global user.email  "contact@datascience.ch"
git config credential.helper "store --file=.git/credentials"
echo "https://${GITHUB_TOKEN}:@github.com" > .git/credentials

# releasing graph-services
sbt "release skip-tests default-tag-exists-answer k  with-defaults"

# build charts/images and push
cd helm-chart
chartpress --push --publish-chart
git diff

# push also images tagged with "latest"
chartpress --tag latest --push

# if it's a tag, push the tagged chart
if [[ -n $TRAVIS_TAG ]]; then
    git clean -dffx
    chartpress --tag $TRAVIS_TAG --push --publish-chart
fi

cd ..

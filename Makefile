# -*- coding: utf-8 -*-
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

DOCKER_REPOSITORY?=renku/
DOCKER_PREFIX:=${DOCKER_REGISTRY}$(DOCKER_REPOSITORY)
DOCKER_LABEL?=$(shell git branch 2> /dev/null | sed -e '/^[^*]/d' -e 's/^* //')

ifeq ($(DOCKER_LABEL), master)
	DOCKER_LABEL=latest
endif

GIT_MASTER_HEAD_SHA:=$(shell git rev-parse --short=7 --verify HEAD)

tag/%: build/%
ifeq (${DOCKER_LABEL}, master)
	docker tag $(DOCKER_PREFIX)$(notdir $@):$(GIT_MASTER_HEAD_SHA) $(DOCKER_PREFIX)$(notdir $@):latest
endif
	docker tag $(DOCKER_PREFIX)$(notdir $@):$(GIT_MASTER_HEAD_SHA) $(DOCKER_PREFIX)$(notdir $@):$(DOCKER_LABEL)

push/%: tag/%
ifeq (${DOCKER_LABEL}, master)
	docker push $(DOCKER_PREFIX)$(notdir $@):latest
endif
	docker push $(DOCKER_PREFIX)$(notdir $@):$(DOCKER_LABEL)
	docker push $(DOCKER_PREFIX)$(notdir $@):$(GIT_MASTER_HEAD_SHA)

login:
	@echo "${DOCKER_PASSWORD}" | docker login -u="${DOCKER_USERNAME}" --password-stdin ${DOCKER_REGISTRY}

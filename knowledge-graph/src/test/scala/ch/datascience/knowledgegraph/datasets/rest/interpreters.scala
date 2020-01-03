/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.IO
import ch.datascience.config.renku
import ch.datascience.knowledgegraph.projects.rest.{ProjectEndpoint, ProjectFinder}
import ch.datascience.logging.ExecutionTimeRecorder
import io.chrisdavenport.log4cats.Logger

class IOProjectEndpointStub(projectFinder:         ProjectFinder[IO],
                            renkuResourcesUrl:     renku.ResourcesUrl,
                            executionTimeRecorder: ExecutionTimeRecorder[IO],
                            logger:                Logger[IO])
    extends ProjectEndpoint[IO](projectFinder, renkuResourcesUrl, executionTimeRecorder, logger)

class IOProjectDatasetsEndpointStub(projectDatasetsFinder: ProjectDatasetsFinder[IO],
                                    renkuResourcesUrl:     renku.ResourcesUrl,
                                    executionTimeRecorder: ExecutionTimeRecorder[IO],
                                    logger:                Logger[IO])
    extends ProjectDatasetsEndpoint[IO](projectDatasetsFinder, renkuResourcesUrl, executionTimeRecorder, logger)

class IODatasetEndpointStub(datasetFinder:         DatasetFinder[IO],
                            renkuResourcesUrl:     renku.ResourcesUrl,
                            executionTimeRecorder: ExecutionTimeRecorder[IO],
                            logger:                Logger[IO])
    extends DatasetEndpoint[IO](datasetFinder, renkuResourcesUrl, executionTimeRecorder, logger)

class IODatasetsSearchEndpointStub(datasetsFinder:        DatasetsFinder[IO],
                                   renkuResourcesUrl:     renku.ResourcesUrl,
                                   executionTimeRecorder: ExecutionTimeRecorder[IO],
                                   logger:                Logger[IO])
    extends DatasetsSearchEndpoint[IO](datasetsFinder, renkuResourcesUrl, executionTimeRecorder, logger)

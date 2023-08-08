/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.entities.viewings.collector
package persons

import io.renku.graph.model.{datasets, persons, projects}
import io.renku.triplesgenerator.api.events.UserId

private[collector] final case class GLUserViewedProject(userId: UserId, project: Project, date: projects.DateViewed)

private[collector] final case class PersonViewedProject(userId:     persons.ResourceId,
                                                        project:    Project,
                                                        dateViewed: projects.DateViewed
)

private[collector] final case class Project(id: projects.ResourceId, slug: projects.Slug)

private[collector] final case class GLUserViewedDataset(userId: UserId, dataset: Dataset, date: datasets.DateViewed)

private[collector] final case class PersonViewedDataset(userId:     persons.ResourceId,
                                                        dataset:    Dataset,
                                                        dateViewed: datasets.DateViewed
)

private[collector] final case class Dataset(id: datasets.ResourceId, identifier: datasets.Identifier)

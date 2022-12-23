/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.entities.searchgraphs

import cats.data.NonEmptyList
import io.renku.graph.model.datasets.{Date, Description, Keyword, Name, ResourceId, TopmostSameAs}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Visibility

private final case class SearchInfo(resourceId:       ResourceId,
                                    topmostSameAs:    TopmostSameAs,
                                    name:             Name,
                                    visibility:       Visibility,
                                    date:             Date,
                                    creators:         NonEmptyList[Person],
                                    keywords:         List[Keyword],
                                    maybeDescription: Option[Description],
                                    images:           List[Image],
                                    projectIds:       List[projects.ResourceId]
)
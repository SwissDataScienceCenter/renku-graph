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

package io.renku.triplesgenerator.events.consumers.syncrepometadata.processor

import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.graph.model.projects
import io.renku.triplesstore.SparqlQuery

private sealed trait DataExtract {
  val path: projects.Path
  val name: projects.Name
}

private object DataExtract {
  final case class TS(id:         projects.ResourceId,
                      path:       projects.Path,
                      name:       projects.Name,
                      visibility: projects.Visibility
  ) extends DataExtract
  final case class GL(path: projects.Path, name: projects.Name, visibility: projects.Visibility) extends DataExtract
  final case class Payload(path: projects.Path, name: projects.Name)                             extends DataExtract
}

private final case class NewValues(maybeName: Option[projects.Name], maybeVisibility: Option[projects.Visibility])
private object NewValues {
  val empty: NewValues = NewValues(maybeName = None, maybeVisibility = None)
}

private sealed trait UpdateCommand extends Product
private object UpdateCommand {
  final case class Sparql(value: SparqlQuery)                                extends UpdateCommand
  final case class Event(value: StatusChangeEvent.RedoProjectTransformation) extends UpdateCommand
}

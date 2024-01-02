/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.entities.searchgraphs.projects.commands

import io.renku.graph.model.GraphClass
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityIdEncoder, Property}
import io.renku.triplesstore.client.model
import io.renku.triplesstore.client.model.{Quad, TripleObject}

private object ProjectsQuad {
  def apply[ID](subject: ID, predicate: Property, obj: TripleObject)(implicit subjectEnc: EntityIdEncoder[ID]): Quad =
    model.Quad(GraphClass.Projects.id, subject.asEntityId, predicate, obj)
}

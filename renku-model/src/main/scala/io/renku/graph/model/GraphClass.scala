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

package io.renku.graph.model

import Schemas.schema
import cats.Show
import io.renku.jsonld.EntityId

sealed trait GraphClass extends Product with Serializable

object GraphClass {

  import io.renku.jsonld.syntax._

  lazy val all: Set[GraphClass] = Set(Default, Project, Persons, Datasets)

  case object Default extends GraphClass
  type Default = Default.type

  case object Project extends GraphClass {
    def id(resourceId: projects.ResourceId): EntityId = resourceId.asEntityId
  }
  type Project = Project.type

  case object Persons extends GraphClass {
    lazy val id: EntityId = EntityId of schema / "Person"
  }
  type Persons = Persons.type

  case object Datasets extends GraphClass {
    lazy val id: EntityId = EntityId of schema / "Dataset"
  }
  type Datasets = Datasets.type

  implicit val show: Show[GraphClass] = Show.show(_.productPrefix)
}

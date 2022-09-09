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

package io.renku.graph.model

import cats.Show

sealed trait Graph extends Product with Serializable

object Graph {

  lazy val all: Set[Graph] = Set(Default, Project, Persons)

  case object Default extends Graph
  type Default = Default.type
  case object Project extends Graph
  type Project = Project.type
  case object Persons extends Graph
  type Persons = Persons.type

  implicit val show: Show[Graph] = Show.show(_.productPrefix)
}

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

package ch.datascience.graph.http.server

import ch.datascience.graph.model.projects

import scala.util.Try

object binders {

  object ProjectId {

    def unapply(value: String): Option[projects.Id] =
      Try {
        projects.Id(value.toInt)
      }.toOption
  }

  object ProjectPath {

    def unapply(value: String): Option[projects.Path] =
      projects.Path.from(value).toOption
  }
}

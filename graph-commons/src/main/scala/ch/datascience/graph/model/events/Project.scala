/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph.model.events

import ch.datascience.tinytypes.constraints.{NonBlank, NonNegative}
import ch.datascience.tinytypes.{IntTinyType, StringTinyType, TinyTypeFactory}
import io.circe.Decoder

case class Project(
    id:   ProjectId,
    path: ProjectPath
)

class ProjectId private (val value: Int) extends AnyVal with IntTinyType
object ProjectId extends TinyTypeFactory[ProjectId](new ProjectId(_)) with NonNegative {
  implicit lazy val projectIdDecoder: Decoder[ProjectId] = Decoder.decodeInt.map(ProjectId.apply)
}

class ProjectPath private (val value: String) extends AnyVal with StringTinyType
object ProjectPath extends TinyTypeFactory[ProjectPath](new ProjectPath(_)) with NonBlank {
  addConstraint(
    check = value =>
      value.contains("/") &&
        (value.indexOf("/") == value.lastIndexOf("/")) &&
        !value.startsWith("/") &&
        !value.endsWith("/"),
    message = (value: String) => s"'$value' is not a valid $typeName"
  )
  implicit lazy val projectPathDecoder: Decoder[ProjectPath] = Decoder.decodeString.map(ProjectPath.apply)
}

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

package ch.datascience.webhookservice.project

import ch.datascience.graph.model.events.ProjectId
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.tinytypes.StringTinyType
import io.circe.Decoder

sealed trait ProjectVisibility extends StringTinyType with Product with Serializable

object ProjectVisibility {

  val all: Set[ProjectVisibility] = Set(Public, Private, Internal)

  implicit lazy val projectVisibilityDecoder: Decoder[ProjectVisibility] =
    Decoder.decodeString.flatMap { decoded =>
      all.find(_.value == decoded) match {
        case Some(value) => Decoder.const(value)
        case None =>
          Decoder.failedWithMessage(
            s"'$decoded' is not a valid project visibility. Allowed values are: ${all.mkString(", ")}"
          )
      }
    }

  final case object Public extends ProjectVisibility {
    override val value: String = "public"
  }

  sealed trait TokenProtectedProject extends ProjectVisibility
  final case object Private extends TokenProtectedProject {
    override val value: String = "private"
  }
  final case object Internal extends TokenProtectedProject {
    override val value: String = "internal"
  }
}

final case class ProjectInfo(
    id:         ProjectId,
    visibility: ProjectVisibility,
    path:       ProjectPath
)

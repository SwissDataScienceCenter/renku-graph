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

package ch.datascience.graph.model

import java.time.Instant

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._

object projects {

  final class ProjectId private (val value: Int) extends AnyVal with IntTinyType
  implicit object ProjectId extends TinyTypeFactory[ProjectId](new ProjectId(_)) with NonNegativeInt

  class ProjectPath private (val value: String) extends AnyVal with RelativePathTinyType
  implicit object ProjectPath extends TinyTypeFactory[ProjectPath](new ProjectPath(_)) with RelativePath {
    private val allowedFirstChar         = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') :+ '_'
    private[projects] val regexValidator = "^([\\w.-]+)(\\/([\\w.-]+))+$"
    addConstraint(
      check   = v => (v contains "/") && (allowedFirstChar contains v.head) && (v matches regexValidator),
      message = (value: String) => s"'$value' is not a valid $typeName"
    )
  }

  class ProjectResource private (val value: String) extends AnyVal with StringTinyType
  implicit object ProjectResource extends TinyTypeFactory[ProjectResource](new ProjectResource(_)) with Url {
    private val regexValidator = s"^http[s]?:\\/\\/.*\\/projects\\/${ProjectPath.regexValidator.drop(1)}"
    addConstraint(
      _ matches regexValidator,
      message = (value: String) => s"'$value' is not a valid $typeName"
    )

    def apply(renkuBaseUrl: RenkuBaseUrl, projectPath: ProjectPath): ProjectResource =
      ProjectResource((renkuBaseUrl / "projects" / projectPath).value)

    private val pathExtractor = "^.*\\/projects\\/(.*)$".r
    implicit lazy val projectPathConverter: TinyTypeConverter[ProjectResource, ProjectPath] = {
      case ProjectResource(pathExtractor(path)) => ProjectPath.from(path)
      case illegalValue                         => Left(new IllegalArgumentException(s"'$illegalValue' cannot be converted to a ProjectPath"))
    }
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class DateCreated private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object DateCreated extends TinyTypeFactory[DateCreated](new DateCreated(_)) with InstantNotInTheFuture

  final class FilePath private (val value: String) extends AnyVal with RelativePathTinyType
  object FilePath extends TinyTypeFactory[FilePath](new FilePath(_)) with RelativePath with RelativePathOps[FilePath]

  sealed trait ProjectVisibility extends StringTinyType with Product with Serializable

  object ProjectVisibility {

    val all: Set[ProjectVisibility] = Set(Public, Private, Internal)

    sealed trait TokenProtectedProject extends ProjectVisibility
    final case object Public           extends ProjectVisibility { override val value: String = "public" }
    final case object Private          extends TokenProtectedProject { override val value: String = "private" }
    final case object Internal         extends TokenProtectedProject { override val value: String = "internal" }

    import io.circe.Decoder

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
  }
}

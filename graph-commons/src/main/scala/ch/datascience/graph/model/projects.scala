/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import io.circe.Decoder
import io.renku.jsonld.EntityId

import java.time.Instant

object projects {

  final class Id private (val value: Int) extends AnyVal with IntTinyType

  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with NonNegativeInt

  class Path private (val value: String) extends AnyVal with RelativePathTinyType

  implicit object Path extends TinyTypeFactory[Path](new Path(_)) with RelativePath {
    private val allowedFirstChar         = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') :+ '_'
    private[projects] val regexValidator = "^([\\w.-]+)(\\/([\\w.-]+))+$"
    addConstraint(
      check = v => (v contains "/") && (allowedFirstChar contains v.head) && (v matches regexValidator),
      message = (value: String) => s"'$value' is not a valid $typeName"
    )
  }

  class ResourceId private (val value: String) extends AnyVal with StringTinyType

  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with UrlResourceRenderer[ResourceId] {

    private val regexValidator = s"^http[s]?:\\/\\/.*\\/projects\\/${Path.regexValidator.drop(1)}"

    addConstraint(
      _ matches regexValidator,
      message = (value: String) => s"'$value' is not a valid $typeName"
    )

    def apply(renkuBaseUrl: RenkuBaseUrl, projectPath: Path): ResourceId =
      ResourceId((renkuBaseUrl / "projects" / projectPath).value)

    def apply(id: EntityId): ResourceId =
      ResourceId(id.value.toString)

    private val pathExtractor = "^.*\\/projects\\/(.*)$".r
    implicit lazy val projectPathConverter: TinyTypeConverter[ResourceId, Path] = {
      case ResourceId(pathExtractor(path)) => Path.from(path)
      case illegalValue                    => Left(new IllegalArgumentException(s"'$illegalValue' cannot be converted to a ProjectPath"))
    }
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType

  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class DateCreated private (val value: Instant) extends AnyVal with InstantTinyType

  implicit object DateCreated extends TinyTypeFactory[DateCreated](new DateCreated(_)) with InstantNotInTheFuture

  final class FilePath private (val value: String) extends AnyVal with RelativePathTinyType

  object FilePath extends TinyTypeFactory[FilePath](new FilePath(_)) with RelativePath with RelativePathOps[FilePath]

  final class Description private (val value: String) extends AnyVal with StringTinyType

  implicit object Description extends TinyTypeFactory[Description](new Description(_)) with NonBlank

  sealed trait Visibility extends StringTinyType with Product with Serializable

  object Visibility extends TinyTypeFactory[Visibility](VisibilityInstantiator) {

    val all: Set[Visibility] = Set(Public, Private, Internal)

    final case object Public extends Visibility {
      override val value: String = "Public"
    }

    final case object Private extends Visibility {
      override val value: String = "Private"
    }

    final case object Internal extends Visibility {
      override val value: String = "Internal"
    }

    import io.circe.Decoder

    implicit lazy val visibilityDecoder: Decoder[Visibility] =
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

  private object VisibilityInstantiator extends (String => Visibility) {
    override def apply(value: String): Visibility = Visibility.all.find(_.value == value).getOrElse {
      throw new IllegalArgumentException(s"'$value' unknown Visibility")
    }
  }
}

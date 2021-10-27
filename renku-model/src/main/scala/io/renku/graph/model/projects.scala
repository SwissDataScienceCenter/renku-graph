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

package io.renku.graph.model

import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.renku.graph.model.views.{AnyResourceRenderer, EntityIdJsonLdOps, TinyTypeJsonLDOps}
import io.renku.jsonld.{EntityId, JsonLDDecoder, JsonLDEncoder}
import io.renku.tinytypes._
import io.renku.tinytypes.constraints._

import java.time.Instant

object projects {

  final class Id private (val value: Int) extends AnyVal with IntTinyType
  implicit object Id extends TinyTypeFactory[Id](new Id(_)) with NonNegativeInt with TinyTypeJsonLDOps[Id]

  final class Path private (val value: String) extends AnyVal with RelativePathTinyType
  implicit object Path extends TinyTypeFactory[Path](new Path(_)) with RelativePath with TinyTypeJsonLDOps[Path] {
    private val allowedFirstChar         = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') :+ '_'
    private[projects] val regexValidator = "^([\\w.-]+)(\\/([\\w.-]+))+$"

    addConstraint(
      check = v => (v contains "/") && (allowedFirstChar contains v.head) && (v matches regexValidator),
      message = (value: String) => s"'$value' is not a valid $typeName"
    )

    implicit class PathOps(path: Path) {
      private lazy val nameString :: namespacesStringReversed = path.show.split('/').toList.reverse
      lazy val toName:       Name            = Name(nameString)
      lazy val toNamespaces: List[Namespace] = namespacesStringReversed.reverseIterator.map(Namespace(_)).toList
    }
  }

  final class Namespace private (val value: String) extends AnyVal with StringTinyType
  object Namespace extends TinyTypeFactory[Namespace](new Namespace(_)) with NonBlank with TinyTypeJsonLDOps[Namespace]

  final class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with AnyResourceRenderer[ResourceId]
      with EntityIdJsonLdOps[ResourceId] {

    private val regexValidator = s"^http[s]?:\\/\\/.*\\/projects\\/${Path.regexValidator.drop(1)}"

    addConstraint(
      _ matches regexValidator,
      message = (value: String) => s"'$value' is not a valid $typeName"
    )

    def apply(projectPath: Path)(implicit renkuBaseUrl: RenkuBaseUrl): ResourceId =
      ResourceId((renkuBaseUrl / "projects" / projectPath).value)

    def apply(id: EntityId): ResourceId = ResourceId(id.value.toString)

    private val pathExtractor = "^.*\\/projects\\/(.*)$".r
    implicit lazy val projectPathConverter: TinyTypeConverter[ResourceId, Path] = {
      case ResourceId(pathExtractor(path)) => Path.from(path)
      case illegalValue => Left(new IllegalArgumentException(s"'$illegalValue' cannot be converted to a ProjectPath"))
    }
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank with TinyTypeJsonLDOps[Name]

  final class DateCreated private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object DateCreated
      extends TinyTypeFactory[DateCreated](new DateCreated(_))
      with InstantNotInTheFuture
      with TinyTypeJsonLDOps[DateCreated]

  final class FilePath private (val value: String) extends AnyVal with RelativePathTinyType
  object FilePath
      extends TinyTypeFactory[FilePath](new FilePath(_))
      with RelativePath
      with RelativePathOps[FilePath]
      with TinyTypeJsonLDOps[FilePath]

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description
      extends TinyTypeFactory[Description](new Description(_))
      with NonBlank
      with TinyTypeJsonLDOps[Description]

  sealed trait Visibility extends StringTinyType with Product with Serializable
  object Visibility extends TinyTypeFactory[Visibility](VisibilityInstantiator) {

    val all: Set[Visibility] = Set(Public, Private, Internal)

    final case object Public extends Visibility {
      override val value: String = "public"
    }

    final case object Private extends Visibility {
      override val value: String = "private"
    }

    final case object Internal extends Visibility {
      override val value: String = "internal"
    }

    import io.circe.Decoder

    implicit lazy val jsonDecoder: Decoder[Visibility] =
      Decoder.decodeString.flatMap { decoded =>
        all.find(_.value == decoded) match {
          case Some(value) => Decoder.const(value)
          case None =>
            Decoder.failedWithMessage(
              s"'$decoded' is not a valid project visibility. Allowed values are: ${all.mkString(", ")}"
            )
        }
      }

    implicit lazy val jsonLDDecoder: JsonLDDecoder[Visibility] =
      JsonLDDecoder.decodeString.emap { decoded =>
        all.find(_.value == decoded) match {
          case Some(value) => value.asRight
          case None => s"'$decoded' is not a valid project visibility. Allowed values are: ${all.mkString(", ")}".asLeft
        }
      }

    implicit lazy val jsonLDEncoder: JsonLDEncoder[Visibility] = JsonLDEncoder.instance {
      case Private  => JsonLDEncoder.encodeString(Private.value)
      case Public   => JsonLDEncoder.encodeString(Public.value)
      case Internal => JsonLDEncoder.encodeString(Internal.value)
    }
  }

  private object VisibilityInstantiator extends (String => Visibility) {
    override def apply(value: String): Visibility = Visibility.all.find(_.value == value).getOrElse {
      throw new IllegalArgumentException(s"'$value' unknown Visibility")
    }
  }

  sealed trait ForksCount extends Any with IntTinyType

  object ForksCount {

    def apply(count: Int Refined Positive): NonZero = NonZero(count.value)

    case object Zero extends ForksCount { override val value: Int = 0 }
    type Zero = Zero.type

    final class NonZero private (val value: Int) extends AnyVal with ForksCount
    object NonZero                               extends TinyTypeFactory[NonZero](new NonZero(_)) with PositiveInt
  }
}

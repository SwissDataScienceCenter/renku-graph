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

import cats.syntax.all._
import ch.datascience.graph.model.Schemas.renku
import ch.datascience.graph.model.entityModel.{Location, LocationLike}
import ch.datascience.graph.model.views.EntityIdJsonLdOps
import ch.datascience.graph.model.views.TinyTypeJsonLDEncoders._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{NonBlank, PositiveInt, Url}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty
import io.circe.DecodingFailure
import io.renku.jsonld._
import io.renku.jsonld.syntax.JsonEncoderOps

object commandParameters {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with EntityIdJsonLdOps[ResourceId]

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class Position private (val value: Int) extends AnyVal with IntTinyType
  implicit object Position extends TinyTypeFactory[Position](new Position(_)) with PositiveInt {
    val first:  Position = Position(1)
    val second: Position = Position(2)
    val third:  Position = Position(3)
  }

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description extends TinyTypeFactory[Description](new Description(_)) with NonBlank

  final class EncodingFormat private (val value: String) extends AnyVal with StringTinyType
  implicit object EncodingFormat extends TinyTypeFactory[EncodingFormat](new EncodingFormat(_)) with NonBlank

  final class Temporary private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object Temporary extends TinyTypeFactory[Temporary](new Temporary(_)) {
    val temporary:    Temporary = Temporary(true)
    val nonTemporary: Temporary = Temporary(false)
  }

  final class Prefix private (val value: String) extends AnyVal with StringTinyType
  implicit object Prefix extends TinyTypeFactory[Prefix](new Prefix(_)) with NonBlank

  final case class InputDefaultValue(value: LocationLike) extends TinyType { type V = LocationLike }
  object InputDefaultValue {
    implicit val jsonLDEncoder: JsonLDEncoder[InputDefaultValue] = JsonLDEncoder[LocationLike].contramap(_.value)

    implicit val jsonLDDecoder: JsonLDDecoder[InputDefaultValue] =
      JsonLDDecoder[Location.FileOrFolder].emap(InputDefaultValue(_).asRight[String])
  }

  final case class OutputDefaultValue(value: LocationLike) extends TinyType { type V = LocationLike }
  implicit object OutputDefaultValue {
    implicit val jsonLDEncoder: JsonLDEncoder[OutputDefaultValue] = JsonLDEncoder[LocationLike].contramap(_.value)

    implicit val jsonLDDecoder: JsonLDDecoder[OutputDefaultValue] =
      JsonLDDecoder[Location.FileOrFolder].emap(OutputDefaultValue(_).asRight[String])
  }

  final class FolderCreation private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object FolderCreation extends TinyTypeFactory[FolderCreation](new FolderCreation(_)) {
    val no:  FolderCreation = FolderCreation(false)
    val yes: FolderCreation = FolderCreation(true)
  }

  final class ParameterDefaultValue private (val value: String) extends AnyVal with StringTinyType
  implicit object ParameterDefaultValue
      extends TinyTypeFactory[ParameterDefaultValue](new ParameterDefaultValue(_))
      with NonBlank

  sealed abstract class IOStream(val resourceId: IOStream.ResourceId, val name: String Refined NonEmpty) {
    override lazy val toString: String = name.toString()
  }
  object IOStream {

    class ResourceId private (val value: String) extends AnyVal with StringTinyType
    implicit object ResourceId
        extends TinyTypeFactory[ResourceId](new ResourceId(_))
        with Url
        with EntityIdJsonLdOps[ResourceId]

    sealed trait In  extends IOStream
    sealed trait Out extends IOStream
    case class StdIn(override val resourceId: ResourceId) extends IOStream(resourceId, StdIn.name) with In
    object StdIn { val name: String Refined NonEmpty = "stdin" }

    case class StdOut(override val resourceId: ResourceId) extends IOStream(resourceId, StdOut.name) with Out
    object StdOut { val name: String Refined NonEmpty = "stdout" }

    case class StdErr(override val resourceId: ResourceId) extends IOStream(resourceId, StdErr.name) with Out
    object StdErr { val name: String Refined NonEmpty = "stderr" }

    private val entityTypes = EntityTypes of renku / "IOStream"

    implicit def encoder[IO <: IOStream]: JsonLDEncoder[IO] =
      JsonLDEncoder.instance[IO] { stream =>
        JsonLD.entity(
          stream.resourceId.asEntityId,
          entityTypes,
          renku / "streamType" -> stream.name.toString().asJsonLD
        )
      }

    implicit lazy val stdInDecoder: JsonLDDecoder[IOStream.In] =
      JsonLDDecoder.entity(entityTypes) { cursor =>
        import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._
        for {
          resourceId <- cursor.downEntityId.as[ResourceId]
          _ <- cursor.downField(renku / "streamType").as[String] >>= {
                 case StdIn.name.value => ().asRight
                 case name             => DecodingFailure(s"$name is cannot be decoded to ${StdIn.name}", Nil).asLeft
               }
        } yield StdIn(resourceId): IOStream.In
      }

    implicit lazy val stdOutDecoder: JsonLDDecoder[IOStream.Out] =
      JsonLDDecoder.entity(entityTypes) { cursor =>
        import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders._
        for {
          resourceId <- cursor.downEntityId.as[ResourceId]
          stdOut <- cursor.downField(renku / "streamType").as[String] >>= {
                      case StdOut.name.value => StdOut(resourceId).asRight
                      case StdErr.name.value => StdErr(resourceId).asRight
                      case name              => DecodingFailure(s"$name is cannot be decoded to ${StdIn.name}", Nil).asLeft
                    }
        } yield stdOut
      }
  }
}

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

import java.time.{Instant, LocalDate}
import java.util.UUID

import ch.datascience.graph.Schemas._
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import io.renku.jsonld._
import io.renku.jsonld.syntax._

object datasets {

  sealed trait DatasetIdentifier extends Any with StringTinyType

  trait DatasetIdentifierFactory[T <: DatasetIdentifier] {
    self: TinyTypeFactory[T] =>
    def apply(id: DatasetIdentifier): T = apply(id.toString)
  }

  final class Identifier private (val value: String) extends AnyVal with DatasetIdentifier
  implicit object Identifier
      extends TinyTypeFactory[Identifier](new Identifier(_))
      with DatasetIdentifierFactory[Identifier]
      with NonBlank

  final class InitialVersion private (val value: String) extends AnyVal with DatasetIdentifier
  implicit object InitialVersion
      extends TinyTypeFactory[InitialVersion](new InitialVersion(_))
      with DatasetIdentifierFactory[InitialVersion]
      with NonBlank

  final class Title private (val value: String) extends AnyVal with StringTinyType
  implicit object Title extends TinyTypeFactory[Title](new Title(_)) with NonBlank

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description extends TinyTypeFactory[Description](new Description(_)) with NonBlank

  final class Keyword private (val value: String) extends AnyVal with StringTinyType
  implicit object Keyword extends TinyTypeFactory[Keyword](new Keyword(_)) with NonBlank

  final class Url private (val value: String) extends AnyVal with StringTinyType
  implicit object Url extends TinyTypeFactory[Url](new Url(_)) with constraints.Url

  final class DerivedFrom private (val value: String) extends AnyVal with StringTinyType
  implicit object DerivedFrom extends TinyTypeFactory[DerivedFrom](new DerivedFrom(_)) with constraints.Url {

    def apply(datasetEntityId: EntityId): DerivedFrom = DerivedFrom(datasetEntityId.toString)

    implicit val derivedFromJsonLdEncoder: JsonLDEncoder[DerivedFrom] = JsonLDEncoder.instance { derivedFrom =>
      JsonLD.entity(
        EntityId of s"_:${UUID.randomUUID()}",
        EntityTypes of (schema / "URL"),
        schema / "url" -> EntityId.of(derivedFrom.value).asJsonLD.flatten.fold(throw _, identity)
      )
    }
  }

  final class TopmostDerivedFrom private[datasets] (val value: String) extends AnyVal with UrlTinyType

  implicit object TopmostDerivedFrom
      extends TinyTypeFactory[TopmostDerivedFrom](new TopmostDerivedFrom(_))
      with constraints.Url {

    final def apply(derivedFrom: DerivedFrom): TopmostDerivedFrom = apply(derivedFrom.value)

    final def apply(entityId: EntityId): TopmostDerivedFrom = apply(entityId.toString)

    implicit lazy val topmostDerivedFromJsonLdEncoder: JsonLDEncoder[TopmostDerivedFrom] =
      derivedFrom => EntityId.of(derivedFrom.value).asJsonLD.flatten.fold(throw _, identity)
  }

  sealed trait SameAs extends Any with UrlTinyType {

    override def equals(obj: Any): Boolean =
      Option(obj).exists {
        case v: SameAs => v.value == value
        case _ => false
      }

    override def hashCode(): Int = value.hashCode
  }
  final class IdSameAs private[datasets] (val value: String) extends SameAs
  final class UrlSameAs private[datasets] (val value: String) extends SameAs
  implicit object SameAs extends TinyTypeFactory[SameAs](new UrlSameAs(_)) with constraints.Url {

    final def fromId(value: String): Either[IllegalArgumentException, IdSameAs] =
      from(value) map (sameAs => new IdSameAs(sameAs.value))

    final def fromUrl(value: String): Either[IllegalArgumentException, UrlSameAs] =
      from(value) map (sameAs => new UrlSameAs(sameAs.value))

    def apply(datasetEntityId: EntityId): IdSameAs = new IdSameAs(datasetEntityId.toString)

    implicit val sameAsJsonLdEncoder: JsonLDEncoder[SameAs] = JsonLDEncoder.instance {
      case v: IdSameAs  => idSameAsJsonLdEncoder(v)
      case v: UrlSameAs => urlSameAsJsonLdEncoder(v)
    }

    private lazy val idSameAsJsonLdEncoder: JsonLDEncoder[IdSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        EntityId of s"_:${UUID.randomUUID()}",
        EntityTypes of (schema / "URL"),
        schema / "url" -> EntityId.of(sameAs.value).asJsonLD.flatten.fold(throw _, identity)
      )
    }

    private lazy val urlSameAsJsonLdEncoder: JsonLDEncoder[UrlSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        EntityId of s"_:${UUID.randomUUID()}",
        EntityTypes of (schema / "URL"),
        schema / "url" -> sameAs.value.asJsonLD.flatten.fold(throw _, identity)
      )
    }
  }
  final class TopmostSameAs private[datasets] (val value: String) extends AnyVal with UrlTinyType
  implicit object TopmostSameAs extends TinyTypeFactory[TopmostSameAs](new TopmostSameAs(_)) with constraints.Url {

    final def apply(sameAs: SameAs): TopmostSameAs = apply(sameAs.value)

    final def apply(entityId: EntityId): TopmostSameAs = apply(entityId.toString)

    implicit lazy val topmostSameAsJsonLdEncoder: JsonLDEncoder[TopmostSameAs] =
      sameAs => EntityId.of(sameAs.value).asJsonLD.flatten.fold(throw _, identity)
  }

  final class DateCreatedInProject private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object DateCreatedInProject
      extends TinyTypeFactory[DateCreatedInProject](new DateCreatedInProject(_))
      with InstantNotInTheFuture

  final class DateCreated private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object DateCreated extends TinyTypeFactory[DateCreated](new DateCreated(_)) with InstantNotInTheFuture

  final class PublishedDate private (val value: LocalDate) extends AnyVal with LocalDateTinyType
  implicit object PublishedDate
      extends TinyTypeFactory[PublishedDate](new PublishedDate(_))
      with LocalDateNotInTheFuture

  final class PartName private (val value: String) extends AnyVal with StringTinyType
  implicit object PartName extends TinyTypeFactory[PartName](new PartName(_)) with NonBlank

  final class PartLocation private (val value: String) extends AnyVal with RelativePathTinyType
  implicit object PartLocation extends TinyTypeFactory[PartLocation](new PartLocation(_)) with RelativePath
}

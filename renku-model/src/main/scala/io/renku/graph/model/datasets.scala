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

import Schemas._
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string
import io.circe._
import io.circe.syntax._
import io.renku.graph.model.views._
import io.renku.jsonld.JsonLDDecoder.{decodeEntityId, decodeString}
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld._
import io.renku.jsonld.syntax._
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.{InstantNotInTheFuture, LocalDateNotInTheFuture, NonBlank, NonNegativeInt, UUID, Url => UrlConstraint}

import java.time.{Instant, LocalDate, ZoneOffset}

object datasets {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with UrlConstraint[ResourceId]
      with EntityIdJsonLdOps[ResourceId]
      with AnyResourceRenderer[ResourceId]

  sealed trait DatasetIdentifier extends Any with StringTinyType

  trait DatasetIdentifierFactory[T <: DatasetIdentifier] {
    self: TinyTypeFactory[T] =>
    def apply(id: DatasetIdentifier): T = apply(id.toString)
  }

  final class Identifier private (val value: String) extends AnyVal with DatasetIdentifier
  implicit object Identifier
      extends TinyTypeFactory[Identifier](new Identifier(_))
      with DatasetIdentifierFactory[Identifier]
      with TinyTypeJsonLDOps[Identifier]
      with NonBlank[Identifier]

  final class OriginalIdentifier private (val value: String) extends AnyVal with DatasetIdentifier
  implicit object OriginalIdentifier
      extends TinyTypeFactory[OriginalIdentifier](new OriginalIdentifier(_))
      with DatasetIdentifierFactory[OriginalIdentifier]
      with TinyTypeJsonLDOps[OriginalIdentifier]
      with NonBlank[OriginalIdentifier]

  final class Title private (val value: String) extends AnyVal with StringTinyType
  implicit object Title extends TinyTypeFactory[Title](new Title(_)) with NonBlank[Title] with TinyTypeJsonLDOps[Title]

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank[Name] with TinyTypeJsonLDOps[Name]

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description
      extends TinyTypeFactory[Description](new Description(_))
      with NonBlank[Description]
      with TinyTypeJsonLDOps[Description]

  final class License private (val value: String) extends AnyVal with StringTinyType
  implicit object License
      extends TinyTypeFactory[License](new License(_))
      with NonBlank[License]
      with TinyTypeJsonLDOps[License]

  final class Version private (val value: String) extends AnyVal with StringTinyType
  implicit object Version
      extends TinyTypeFactory[Version](new Version(_))
      with NonBlank[Version]
      with TinyTypeJsonLDOps[Version]

  final class Keyword private (val value: String) extends AnyVal with StringTinyType
  implicit object Keyword
      extends TinyTypeFactory[Keyword](new Keyword(_))
      with NonBlank[Keyword]
      with TinyTypeJsonLDOps[Keyword]

  class ImageResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ImageResourceId
      extends TinyTypeFactory[ImageResourceId](new ImageResourceId(_))
      with UrlConstraint[ImageResourceId]
      with EntityIdJsonLdOps[ImageResourceId]

  final class ImagePosition private (val value: Int) extends AnyVal with IntTinyType
  implicit object ImagePosition
      extends TinyTypeFactory[ImagePosition](new ImagePosition(_))
      with NonNegativeInt[ImagePosition]
      with TinyTypeJsonLDOps[ImagePosition]

  trait ImageUri extends Any with TinyType { type V = String }
  object ImageUri extends From[ImageUri] with TinyTypeJsonLDOps[ImageUri] {

    def apply(value: String): ImageUri = from(value).fold(throw _, identity)

    override def from(value: String): Either[IllegalArgumentException, ImageUri] =
      Relative.from(value) orElse Absolute.from(value)

    final class Relative private (val value: String) extends AnyVal with ImageUri with RelativePathTinyType {
      override type V = String
    }
    implicit object Relative extends TinyTypeFactory[Relative](new Relative(_)) with constraints.RelativePath[Relative]

    final class Absolute private (val value: String) extends AnyVal with ImageUri with UrlTinyType {
      override type V = String
    }
    implicit object Absolute extends TinyTypeFactory[Absolute](new Absolute(_)) with constraints.Url[Absolute]

    import io.renku.tinytypes.json.TinyTypeEncoders._

    implicit lazy val encoder: Encoder[ImageUri] = Encoder.instance {
      case uri: Relative => Json.fromString(uri.value)
      case uri: Absolute => uri.asJson
    }

    implicit lazy val decoder: Decoder[ImageUri] = Decoder.decodeString.emap { value =>
      (Relative.from(value) orElse Absolute.from(value)).leftMap(_ => s"Cannot decode $value to $ImageUri")
    }
  }

  final class PartLocation private (val value: String) extends AnyVal with StringTinyType
  implicit object PartLocation
      extends TinyTypeFactory[PartLocation](new PartLocation(_))
      with constraints.RelativePath[PartLocation]
      with TinyTypeJsonLDOps[PartLocation]

  final class DerivedFrom private (val value: String) extends AnyVal with StringTinyType
  implicit object DerivedFrom
      extends TinyTypeFactory[DerivedFrom](new DerivedFrom(_))
      with constraints.Url[DerivedFrom]
      with AnyResourceRenderer[DerivedFrom] {

    def apply(datasetEntityId: EntityId): DerivedFrom = DerivedFrom(datasetEntityId.toString)

    private val entityTypes = EntityTypes of (schema / "URL")

    implicit val jsonLDEncoder: JsonLDEncoder[DerivedFrom] = JsonLDEncoder.instance { derivedFrom =>
      JsonLD.entity(
        EntityId of s"$derivedFrom/${derivedFrom.value.hashCode}",
        entityTypes,
        schema / "url" -> EntityId.of(derivedFrom.value).asJsonLD
      )
    }

    implicit lazy val jsonLDDecoder: JsonLDDecoder[DerivedFrom] = JsonLDDecoder.entity(entityTypes) {
      _.downField(schema / "url").downEntityId.as[EntityId].map(DerivedFrom(_))
    }
  }

  final class TopmostDerivedFrom private[datasets] (val value: String) extends AnyVal with UrlTinyType
  implicit object TopmostDerivedFrom
      extends TinyTypeFactory[TopmostDerivedFrom](new TopmostDerivedFrom(_))
      with constraints.Url[TopmostDerivedFrom]
      with UrlResourceRenderer[TopmostDerivedFrom] {

    final def apply(derivedFrom: DerivedFrom): TopmostDerivedFrom = apply(derivedFrom.value)

    final def apply(entityId: EntityId): TopmostDerivedFrom = apply(entityId.toString)

    implicit lazy val topmostDerivedFromJsonLdEncoder: JsonLDEncoder[TopmostDerivedFrom] =
      derivedFrom => EntityId.of(derivedFrom.value).asJsonLD
  }

  sealed trait SameAs extends Any with UrlTinyType {

    override def equals(obj: Any): Boolean =
      Option(obj).exists {
        case v: SameAs => v.value == value
        case _ => false
      }

    override def hashCode(): Int = value.hashCode
  }

  final class InternalSameAs private[datasets] (val value: String) extends SameAs
  final class ExternalSameAs private[datasets] (val value: String) extends SameAs

  implicit object InternalSameAs
      extends TinyTypeFactory[InternalSameAs](new InternalSameAs(_))
      with constraints.Url[InternalSameAs] {

    implicit class InternalSameAsOps(internalSameAs: InternalSameAs) {
      lazy val asIdentifier: Identifier = internalSameAs.value match {
        case s"$_/datasets/$identifier" => Identifier(identifier)
        case url                        => throw new Exception(s"Unknown sameAs URL pattern: $url")
      }
    }
  }

  object ExternalSameAs
      extends TinyTypeFactory[ExternalSameAs](new ExternalSameAs(_))
      with constraints.Url[ExternalSameAs]

  implicit object SameAs
      extends TinyTypeFactory[SameAs](new ExternalSameAs(_))
      with constraints.Url[SameAs]
      with UrlResourceRenderer[SameAs] {

    final def internal(value: RenkuUrl): Either[IllegalArgumentException, InternalSameAs] =
      from(value.value) map (sameAs => new InternalSameAs(sameAs.value))

    final def external(value: String Refined string.Url): Either[IllegalArgumentException, ExternalSameAs] =
      from(value.value) map (sameAs => new ExternalSameAs(sameAs.value))

    def apply(datasetEntityId: EntityId): InternalSameAs = new InternalSameAs(datasetEntityId.toString)

    implicit lazy val jsonLdEncoder: JsonLDEncoder[SameAs] = JsonLDEncoder.instance {
      case sameAs @ InternalSameAs(_) => internalSameAsEncoder(sameAs)
      case sameAs @ ExternalSameAs(_) => externalSameAsEncoder(sameAs)
    }

    private val urlEntityTypes = EntityTypes of (schema / "URL")

    implicit lazy val internalSameAsEncoder: JsonLDEncoder[InternalSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        EntityId of s"$sameAs/${sameAs.value.hashCode}",
        urlEntityTypes,
        schema / "url" -> EntityId.of(sameAs.value).asJsonLD
      )
    }

    implicit lazy val internalSameAsDecoder: JsonLDDecoder[InternalSameAs] = JsonLDDecoder.entity(urlEntityTypes) {
      _.downField(schema / "url").downEntityId.as[EntityId].map(SameAs(_))
    }

    implicit lazy val externalSameAsEncoder: JsonLDEncoder[ExternalSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        EntityId of s"$sameAs/${sameAs.value.hashCode}",
        urlEntityTypes,
        schema / "url" -> sameAs.value.asJsonLD
      )
    }

    implicit lazy val externalSameAsDecoder: JsonLDDecoder[ExternalSameAs] = JsonLDDecoder.entity(urlEntityTypes) {
      _.downField(schema / "url")
        .as[String]
        .flatMap(url => SameAs.external(Refined.unsafeApply(url)).leftMap(err => DecodingFailure(err.getMessage, Nil)))
    }
  }

  final class TopmostSameAs private[datasets] (val value: String) extends AnyVal with UrlTinyType
  implicit object TopmostSameAs
      extends TinyTypeFactory[TopmostSameAs](new TopmostSameAs(_))
      with constraints.Url[TopmostSameAs]
      with UrlResourceRenderer[TopmostSameAs] {

    final def apply(sameAs: SameAs): TopmostSameAs = apply(sameAs.value)

    final def apply(entityId: EntityId): TopmostSameAs = apply(entityId.toString)

    implicit lazy val topmostSameAsJsonLdEncoder: JsonLDEncoder[TopmostSameAs] =
      sameAs => EntityId.of(sameAs.value).asJsonLD
  }

  class PartResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object PartResourceId
      extends TinyTypeFactory[PartResourceId](new PartResourceId(_))
      with UrlConstraint[PartResourceId]
      with EntityIdJsonLdOps[PartResourceId]

  sealed trait Date extends Any with TinyType {
    def instant: Instant
  }

  object Date {
    import io.renku.tinytypes.json.TinyTypeEncoders._
    implicit val encoder: Encoder[Date] = Encoder.instance {
      case d: DateCreated   => d.asJson
      case d: DatePublished => d.asJson
    }
  }

  final class DateCreated private (val value: Instant) extends AnyVal with Date with InstantTinyType {
    override def instant: Instant = value
  }
  implicit object DateCreated
      extends TinyTypeFactory[DateCreated](new DateCreated(_))
      with InstantNotInTheFuture[DateCreated]
      with TinyTypeJsonLDOps[DateCreated]

  final class DatePublished private (val value: LocalDate) extends AnyVal with Date with LocalDateTinyType {
    override def instant: Instant = value.atStartOfDay().toInstant(ZoneOffset.UTC)
  }
  implicit object DatePublished
      extends TinyTypeFactory[DatePublished](new DatePublished(_))
      with LocalDateNotInTheFuture[DatePublished]
      with TinyTypeJsonLDOps[DatePublished]

  final class PartId private (val value: String) extends AnyVal with StringTinyType
  implicit object PartId extends TinyTypeFactory[PartId](new PartId(_)) with UUID[PartId] with TinyTypeJsonLDOps[PartId]

  final class PartExternal private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object PartExternal
      extends TinyTypeFactory[PartExternal](new PartExternal(_))
      with TinyTypeJsonLDOps[PartExternal] {
    val yes: PartExternal = PartExternal(true)
    val no:  PartExternal = PartExternal(false)
  }

  final class PartSource private (val value: String) extends AnyVal with UrlTinyType
  implicit object PartSource extends TinyTypeFactory[PartSource](new PartSource(_)) {
    implicit lazy val jsonLDEncoder: JsonLDEncoder[PartSource] =
      JsonLDEncoder.instance(v => JsonLD.fromString(v.value))
    implicit lazy val jsonLDDecoder: JsonLDDecoder[PartSource] =
      decodeString.emap(value => from(value).leftMap(_.getMessage))
  }
}

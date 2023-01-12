/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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
import cats.Show
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string
import io.circe._
import io.circe.syntax._
import io.renku.graph.model.entities.Dataset
import io.renku.graph.model.views._
import io.renku.jsonld.JsonLDDecoder.{decodeEntityId, decodeString}
import io.renku.jsonld.JsonLDEncoder._
import io.renku.jsonld._
import io.renku.jsonld.ontology.{Class, ObjectProperty, Type}
import io.renku.jsonld.syntax._
import io.renku.tinytypes._
import io.renku.tinytypes.constraints.{InstantNotInTheFuture, LocalDateNotInTheFuture, NonBlank, UUID, Url => UrlConstraint}

import java.time.{Instant, LocalDate, ZoneOffset}

object datasets {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with UrlConstraint[ResourceId]
      with EntityIdJsonLDOps[ResourceId]
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

    val ontology: Type = Type.Def(
      Class(schema / "URL"),
      ObjectProperty(schema / "url", Dataset.ontologyClass)
    )
  }

  final class TopmostDerivedFrom private[datasets] (val value: String) extends AnyVal with UrlTinyType
  implicit object TopmostDerivedFrom
      extends TinyTypeFactory[TopmostDerivedFrom](new TopmostDerivedFrom(_))
      with constraints.Url[TopmostDerivedFrom]
      with UrlResourceRenderer[TopmostDerivedFrom]
      with EntityIdJsonLDOps[TopmostDerivedFrom] {

    final def apply(derivedFrom: DerivedFrom): TopmostDerivedFrom = apply(derivedFrom.value)

    final def apply(entityId: EntityId): TopmostDerivedFrom = apply(entityId.toString)
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

    implicit lazy val jsonLDEncoder: JsonLDEncoder[SameAs] = JsonLDEncoder.instance {
      case sameAs @ InternalSameAs(_) => internalSameAsEncoder(sameAs)
      case sameAs @ ExternalSameAs(_) => externalSameAsEncoder(sameAs)
    }

    implicit def jsonLDEntityEncoder[S <: SameAs]: EntityIdEncoder[S] = EntityIdEncoder.instance {
      case sameAs @ InternalSameAs(_) => EntityId of s"$sameAs/${sameAs.value.hashCode}"
      case sameAs @ ExternalSameAs(_) => EntityId of s"$sameAs/${sameAs.value.hashCode}"
    }

    private val urlEntityTypes = EntityTypes of (schema / "URL")

    implicit lazy val internalSameAsEncoder: JsonLDEncoder[InternalSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        sameAs.asEntityId,
        urlEntityTypes,
        schema / "url" -> EntityId.of(sameAs.value).asJsonLD
      )
    }

    implicit lazy val internalSameAsDecoder: JsonLDDecoder[InternalSameAs] = JsonLDDecoder.entity(urlEntityTypes) {
      _.downField(schema / "url").downEntityId.as[EntityId].map(SameAs(_))
    }

    implicit lazy val externalSameAsEncoder: JsonLDEncoder[ExternalSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        sameAs.asEntityId,
        urlEntityTypes,
        schema / "url" -> sameAs.value.asJsonLD
      )
    }

    implicit lazy val externalSameAsDecoder: JsonLDDecoder[ExternalSameAs] = JsonLDDecoder.entity(urlEntityTypes) {
      _.downField(schema / "url")
        .as[String]
        .flatMap(url => SameAs.external(Refined.unsafeApply(url)).leftMap(err => DecodingFailure(err.getMessage, Nil)))
    }

    val ontology: Type = Type.Def(
      Class(schema / "URL"),
      ObjectProperty(schema / "url", Dataset.ontologyClass)
    )
  }

  final class TopmostSameAs private[datasets] (val value: String) extends AnyVal with UrlTinyType
  implicit object TopmostSameAs
      extends TinyTypeFactory[TopmostSameAs](new TopmostSameAs(_))
      with constraints.Url[TopmostSameAs]
      with UrlResourceRenderer[TopmostSameAs]
      with EntityIdJsonLDOps[TopmostSameAs] {

    final def apply(sameAs: SameAs): TopmostSameAs = apply(sameAs.value)

    final def apply(entityId: EntityId): TopmostSameAs = apply(entityId.toString)
  }

  class PartResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object PartResourceId
      extends TinyTypeFactory[PartResourceId](new PartResourceId(_))
      with UrlConstraint[PartResourceId]
      with EntityIdJsonLDOps[PartResourceId]

  sealed trait Date extends Any with TinyType {
    def instant: Instant
  }

  object Date {
    implicit val encoder: Encoder[Date] = Encoder.instance {
      case d: DateCreated   => d.asJson
      case d: DatePublished => d.asJson
    }

    implicit val show: Show[Date] =
      Show.show {
        case d: DateCreated   => d.show
        case d: DatePublished => d.show
      }
  }

  final class DateCreated private (val value: Instant) extends AnyVal with Date with InstantTinyType {
    override def instant: Instant = value
  }
  implicit object DateCreated
      extends TinyTypeFactory[DateCreated](new DateCreated(_))
      with InstantNotInTheFuture[DateCreated]
      with TinyTypeJsonLDOps[DateCreated] {
    implicit override lazy val show: Show[DateCreated] =
      Show.show(d => s"DateCreated(${d.value})")
  }

  final class DatePublished private (val value: LocalDate) extends AnyVal with Date with LocalDateTinyType {
    override def instant: Instant = value.atStartOfDay().toInstant(ZoneOffset.UTC)
  }
  implicit object DatePublished
      extends TinyTypeFactory[DatePublished](new DatePublished(_))
      with LocalDateNotInTheFuture[DatePublished]
      with TinyTypeJsonLDOps[DatePublished] {
    implicit override lazy val show: Show[DatePublished] =
      Show.show(d => s"DatePublished(${d.value})")
  }

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

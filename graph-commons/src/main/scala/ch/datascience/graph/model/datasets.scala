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

import ch.datascience.graph.Schemas._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string
import io.renku.jsonld._
import io.renku.jsonld.syntax._

import java.time.{Instant, LocalDate, ZoneOffset}

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

  final class License private (val value: String) extends AnyVal with StringTinyType
  implicit object License extends TinyTypeFactory[License](new License(_)) with NonBlank

  final class Version private (val value: String) extends AnyVal with StringTinyType
  implicit object Version extends TinyTypeFactory[Version](new Version(_)) with NonBlank

  final class Keyword private (val value: String) extends AnyVal with StringTinyType
  implicit object Keyword extends TinyTypeFactory[Keyword](new Keyword(_)) with NonBlank

  final class Url private (val value: String) extends AnyVal with StringTinyType
  implicit object Url extends TinyTypeFactory[Url](new Url(_)) with constraints.Url

  final class ImageUri private (val value: String) extends AnyVal with StringTinyType
  implicit object ImageUri extends TinyTypeFactory[ImageUri](new ImageUri(_)) with constraints.RelativePath

  final class PartLocation private (val value: String) extends AnyVal with StringTinyType
  implicit object PartLocation extends TinyTypeFactory[PartLocation](new PartLocation(_)) with constraints.RelativePath

  final class DerivedFrom private (val value: String) extends AnyVal with StringTinyType
  implicit object DerivedFrom extends TinyTypeFactory[DerivedFrom](new DerivedFrom(_)) with constraints.Url {

    def apply(datasetEntityId: EntityId): DerivedFrom = DerivedFrom(datasetEntityId.toString)

    implicit val derivedFromJsonLdEncoder: JsonLDEncoder[DerivedFrom] = JsonLDEncoder.instance { derivedFrom =>
      JsonLD.entity(
        EntityId of s"_:${java.util.UUID.randomUUID()}",
        EntityTypes of (schema / "URL"),
        schema / "url" -> EntityId.of(derivedFrom.value).asJsonLD
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

  implicit object InternalSameAs extends TinyTypeFactory[InternalSameAs](new InternalSameAs(_)) with constraints.Url {
    implicit class InternalSameAsOps(internalSameAs: InternalSameAs) {
      lazy val asIdentifier: Identifier = internalSameAs.value match {
        case s"$_/datasets/$identifier" => Identifier(identifier)
        case url                        => throw new Exception(s"Unknown sameAs URL pattern: $url")
      }

    }
  }

  implicit object SameAs extends TinyTypeFactory[SameAs](new ExternalSameAs(_)) with constraints.Url {

    final def internal(value: RenkuBaseUrl): Either[IllegalArgumentException, InternalSameAs] =
      from(value.value) map (sameAs => new InternalSameAs(sameAs.value))

    final def external(value: String Refined string.Url): Either[IllegalArgumentException, ExternalSameAs] =
      from(value.value) map (sameAs => new ExternalSameAs(sameAs.value))

    def apply(datasetEntityId: EntityId): InternalSameAs = new InternalSameAs(datasetEntityId.toString)

    implicit val sameAsJsonLdEncoder: JsonLDEncoder[SameAs] = JsonLDEncoder.instance {
      case v: InternalSameAs => internalSameAsEncoder(v)
      case v: ExternalSameAs => externalSameAsEncoder(v)
    }

    private lazy val internalSameAsEncoder: JsonLDEncoder[InternalSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        EntityId of s"_:${java.util.UUID.randomUUID()}",
        EntityTypes of (schema / "URL"),
        schema / "url" -> EntityId.of(sameAs.value).asJsonLD
      )
    }

    private lazy val externalSameAsEncoder: JsonLDEncoder[ExternalSameAs] = JsonLDEncoder.instance { sameAs =>
      JsonLD.entity(
        EntityId of s"_:${java.util.UUID.randomUUID()}",
        EntityTypes of (schema / "URL"),
        schema / "url" -> sameAs.value.asJsonLD
      )
    }
  }

  final class TopmostSameAs private[datasets] (val value: String) extends AnyVal with UrlTinyType
  implicit object TopmostSameAs extends TinyTypeFactory[TopmostSameAs](new TopmostSameAs(_)) with constraints.Url {

    final def apply(sameAs: SameAs): TopmostSameAs = apply(sameAs.value)

    final def apply(entityId: EntityId): TopmostSameAs = apply(entityId.toString)

    implicit lazy val topmostSameAsJsonLdEncoder: JsonLDEncoder[TopmostSameAs] =
      sameAs => EntityId.of(sameAs.value).asJsonLD
  }

  sealed trait Date extends Any {
    def instant: Instant
  }

  final class DateCreated private (val value: Instant) extends AnyVal with Date with InstantTinyType {
    override def instant: Instant = value
  }
  implicit object DateCreated extends TinyTypeFactory[DateCreated](new DateCreated(_)) with InstantNotInTheFuture

  final class DatePublished private (val value: LocalDate) extends AnyVal with Date with LocalDateTinyType {
    override def instant: Instant = value.atStartOfDay().toInstant(ZoneOffset.UTC)
  }
  implicit object DatePublished
      extends TinyTypeFactory[DatePublished](new DatePublished(_))
      with LocalDateNotInTheFuture

  final class PartId private (val value: String) extends AnyVal with StringTinyType
  implicit object PartId extends TinyTypeFactory[PartId](new PartId(_)) with UUID {
    def generate: PartId = PartId {
      java.util.UUID.randomUUID.toString
    }
  }

  final class PartExternal private (val value: Boolean) extends AnyVal with BooleanTinyType
  implicit object PartExternal extends TinyTypeFactory[PartExternal](new PartExternal(_)) {
    val yes: PartExternal = PartExternal(true)
    val no:  PartExternal = PartExternal(false)
  }

  final class PartSource private (val value: String) extends AnyVal with UrlTinyType
  implicit object PartSource extends TinyTypeFactory[PartSource](new PartSource(_))
}

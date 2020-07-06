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

import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._

object datasets {

  final class Identifier private (val value: String) extends AnyVal with StringTinyType
  implicit object Identifier extends TinyTypeFactory[Identifier](new Identifier(_)) with NonBlank

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class AlternateName private (val value: String) extends AnyVal with StringTinyType
  implicit object AlternateName extends TinyTypeFactory[AlternateName](new AlternateName(_)) with NonBlank

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description extends TinyTypeFactory[Description](new Description(_)) with NonBlank

  final class Keyword private (val value: String) extends AnyVal with StringTinyType
  implicit object Keyword extends TinyTypeFactory[Keyword](new Keyword(_)) with NonBlank

  final class Url private (val value: String) extends AnyVal with StringTinyType
  implicit object Url extends TinyTypeFactory[Url](new Url(_)) with constraints.Url

  sealed trait SameAs extends Any with UrlTinyType {

    override def equals(obj: Any): Boolean =
      Option(obj).exists {
        case v: SameAs => v.value == value
        case _ => false
      }

    override def hashCode(): Int = value.hashCode
  }
  final class IdSameAs private[datasets] (val value:  String) extends SameAs
  final class UrlSameAs private[datasets] (val value: String) extends SameAs
  implicit object SameAs extends TinyTypeFactory[SameAs](new UrlSameAs(_)) with constraints.Url {
    final def fromId(value: String): Either[IllegalArgumentException, SameAs] =
      from(value) map (sameAs => new IdSameAs(sameAs.value))
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

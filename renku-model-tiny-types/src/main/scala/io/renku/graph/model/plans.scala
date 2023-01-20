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

import cats.syntax.all._
import io.renku.graph.model.views.{AnyResourceRenderer, EntityIdJsonLDOps, TinyTypeJsonLDOps}
import io.renku.jsonld.{EntityId, EntityIdEncoder}
import io.renku.jsonld.syntax._
import io.renku.tinytypes.constraints.{InstantNotInTheFuture, NonBlank, NonNegativeInt, Url}
import io.renku.tinytypes._

import java.time.Instant

object plans {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url[ResourceId]
      with EntityIdJsonLDOps[ResourceId]
      with AnyResourceRenderer[ResourceId] {

    def apply(identifier: Identifier)(implicit renkuUrl: RenkuUrl): ResourceId =
      ResourceId((renkuUrl / "plans" / identifier).value)

    private val identifierExtractor = "^.*\\/plans\\/(.*)$".r
    implicit lazy val toIdentifier: TinyTypeConverter[ResourceId, Identifier] = {
      case ResourceId(identifierExtractor(id)) => Identifier(id).asRight
      case unknown => new IllegalArgumentException(s"'$unknown' cannot be converted to a plans.Identifier").asLeft
    }
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank[Name] with TinyTypeJsonLDOps[Name]

  final class Identifier private (val value: String) extends AnyVal with StringTinyType
  implicit object Identifier extends TinyTypeFactory[Identifier](new Identifier(_)) with NonBlank[Identifier] {
    implicit def entityIdEncoder(implicit renkuUrl: RenkuUrl): EntityIdEncoder[Identifier] =
      EntityIdEncoder.instance(id => ResourceId(id).asEntityId)
  }

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description
      extends TinyTypeFactory[Description](new Description(_))
      with NonBlank[Description]
      with TinyTypeJsonLDOps[Description]

  final class Command private (val value: String) extends AnyVal with StringTinyType
  implicit object Command
      extends TinyTypeFactory[Command](new Command(_))
      with NonBlank[Command]
      with TinyTypeJsonLDOps[Command]

  final class Keyword private (val value: String) extends AnyVal with StringTinyType
  implicit object Keyword
      extends TinyTypeFactory[Keyword](new Keyword(_))
      with NonBlank[Keyword]
      with TinyTypeJsonLDOps[Keyword]

  final class ProgrammingLanguage private (val value: String) extends AnyVal with StringTinyType

  implicit object ProgrammingLanguage
      extends TinyTypeFactory[ProgrammingLanguage](new ProgrammingLanguage(_))
      with NonBlank[ProgrammingLanguage]
      with TinyTypeJsonLDOps[ProgrammingLanguage]

  final class SuccessCode private (val value: Int) extends AnyVal with IntTinyType

  implicit object SuccessCode
      extends TinyTypeFactory[SuccessCode](new SuccessCode(_))
      with NonNegativeInt[SuccessCode]
      with TinyTypeJsonLDOps[SuccessCode]

  final class DateCreated private (val value: Instant) extends AnyVal with InstantTinyType

  implicit object DateCreated
      extends TinyTypeFactory[DateCreated](new DateCreated(_))
      with InstantNotInTheFuture[DateCreated]
      with TinyTypeJsonLDOps[DateCreated]

  final class DerivedFrom private (val value: String) extends AnyVal with StringTinyType
  implicit object DerivedFrom
      extends TinyTypeFactory[DerivedFrom](new DerivedFrom(_))
      with constraints.Url[DerivedFrom]
      with AnyResourceRenderer[DerivedFrom]
      with EntityIdJsonLDOps[DerivedFrom] {

    def apply(entityId: EntityId): DerivedFrom = DerivedFrom(entityId.toString)
  }
}

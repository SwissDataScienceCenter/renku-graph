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

import io.renku.graph.model.views.{EntityIdJsonLdOps, TinyTypeJsonLDOps}
import io.renku.tinytypes.constraints.{InstantNotInTheFuture, NonBlank, NonNegativeInt, Url}
import io.renku.tinytypes.{InstantTinyType, IntTinyType, StringTinyType, TinyTypeFactory}

import java.time.Instant

object plans {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType

  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with EntityIdJsonLdOps[ResourceId]

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank with TinyTypeJsonLDOps[Name]

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description
      extends TinyTypeFactory[Description](new Description(_))
      with NonBlank
      with TinyTypeJsonLDOps[Description]

  final class Command private (val value: String) extends AnyVal with StringTinyType
  implicit object Command extends TinyTypeFactory[Command](new Command(_)) with NonBlank with TinyTypeJsonLDOps[Command]

  final class Keyword private (val value: String) extends AnyVal with StringTinyType
  implicit object Keyword extends TinyTypeFactory[Keyword](new Keyword(_)) with NonBlank with TinyTypeJsonLDOps[Keyword]

  final class ProgrammingLanguage private (val value: String) extends AnyVal with StringTinyType

  implicit object ProgrammingLanguage
      extends TinyTypeFactory[ProgrammingLanguage](new ProgrammingLanguage(_))
      with NonBlank
      with TinyTypeJsonLDOps[ProgrammingLanguage]

  final class SuccessCode private (val value: Int) extends AnyVal with IntTinyType

  implicit object SuccessCode
      extends TinyTypeFactory[SuccessCode](new SuccessCode(_))
      with NonNegativeInt
      with TinyTypeJsonLDOps[SuccessCode]

  final class DateCreated private (val value: Instant) extends AnyVal with InstantTinyType

  implicit object DateCreated
      extends TinyTypeFactory[DateCreated](new DateCreated(_))
      with InstantNotInTheFuture
      with TinyTypeJsonLDOps[DateCreated]

}

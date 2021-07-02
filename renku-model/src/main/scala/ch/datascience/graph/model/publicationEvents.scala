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

import ch.datascience.graph.model.views.EntityIdJsonLdOps
import ch.datascience.tinytypes.constraints.{InstantNotInTheFuture, NonBlank, Url}
import ch.datascience.tinytypes.{InstantTinyType, StringTinyType, TinyTypeFactory}

import java.time.Instant

object publicationEvents {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with EntityIdJsonLdOps[ResourceId]

  final class AboutEvent private (val value: String) extends AnyVal with StringTinyType
  implicit object AboutEvent extends TinyTypeFactory[AboutEvent](new AboutEvent(_)) with NonBlank

  final class Description private (val value: String) extends AnyVal with StringTinyType
  implicit object Description extends TinyTypeFactory[Description](new Description(_)) with NonBlank

  final class Location private (val value: String) extends AnyVal with StringTinyType
  implicit object Location extends TinyTypeFactory[Location](new Location(_)) with NonBlank

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank

  final class StartDate private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object StartDate extends TinyTypeFactory[StartDate](new StartDate(_)) with InstantNotInTheFuture
}

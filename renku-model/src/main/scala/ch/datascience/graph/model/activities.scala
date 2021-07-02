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
import ch.datascience.graph.model.views.EntityIdJsonLdOps
import ch.datascience.tinytypes.{InstantTinyType, IntTinyType, StringTinyType, TinyTypeFactory}
import ch.datascience.tinytypes.constraints.{BoundedInstant, PositiveInt, Url}

import java.time.Instant

object activities {

  class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with Url
      with EntityIdJsonLdOps[ResourceId]

  final class StartTime private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object StartTime extends TinyTypeFactory[StartTime](new StartTime(_)) with BoundedInstant {
    import java.time.temporal.ChronoUnit.HOURS
    protected[this] override def maybeMax: Option[Instant] = now.plus(24, HOURS).some
  }

  final class EndTime private (val value: Instant) extends AnyVal with InstantTinyType
  implicit object EndTime extends TinyTypeFactory[EndTime](new EndTime(_)) with BoundedInstant {
    import java.time.temporal.ChronoUnit.HOURS
    protected[this] override def maybeMax: Option[Instant] = now.plus(24, HOURS).some
  }

  final class Order private (val value: Int) extends AnyVal with IntTinyType
  implicit object Order extends TinyTypeFactory[Order](new Order(_)) with PositiveInt
}

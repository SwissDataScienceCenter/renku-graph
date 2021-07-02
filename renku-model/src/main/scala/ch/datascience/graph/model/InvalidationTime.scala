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

import ch.datascience.tinytypes.constraints.BoundedInstant
import ch.datascience.tinytypes.{InstantTinyType, TinyTypeFactory}

import java.time.Instant

final class InvalidationTime private (val value: Instant) extends AnyVal with InstantTinyType
object InvalidationTime extends TinyTypeFactory[InvalidationTime](new InvalidationTime(_)) with BoundedInstant {
  import cats.syntax.all._
  import ch.datascience.graph.model.views.TinyTypeJsonLDDecoders
  import io.renku.jsonld.JsonLDDecoder

  import java.time.temporal.ChronoUnit.HOURS

  protected[this] override def maybeMax: Option[Instant] = now.plus(2, HOURS).some

  implicit lazy val jsonLDDecoder: JsonLDDecoder[InvalidationTime] = TinyTypeJsonLDDecoders.instantDecoder(this)
}

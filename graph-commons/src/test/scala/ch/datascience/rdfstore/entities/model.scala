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

package ch.datascience.rdfstore.entities

import ch.datascience.tinytypes.constraints._
import ch.datascience.tinytypes.{InstantTinyType, RelativePathTinyType, StringTinyType, TinyTypeFactory}
import io.circe.Json
import io.renku.jsonld.EntityId

import java.time.Instant
import cats.syntax.all._

final class Location private (val value: String) extends AnyVal with RelativePathTinyType
object Location extends TinyTypeFactory[Location](new Location(_)) with RelativePath with RelativePathOps[Location]

final class InvalidationTime private (val value: Instant) extends AnyVal with InstantTinyType
object InvalidationTime extends TinyTypeFactory[InvalidationTime](new InvalidationTime(_)) with BoundedInstant {
  import java.time.temporal.ChronoUnit.HOURS
  protected[this] override def maybeMax: Option[Instant] = now.plus(2, HOURS).some
}

final case class UrlfiedEntityId(value: String) extends EntityId with StringTinyType {
  override type Value = String
  override lazy val asJson: Json = Json.fromString(value)
}

object UrlfiedEntityId
    extends TinyTypeFactory[UrlfiedEntityId](new UrlfiedEntityId(_))
    with Url
    with UrlOps[UrlfiedEntityId]
    with UrlResourceRenderer[UrlfiedEntityId] {

  implicit val jsonEncoder: UrlfiedEntityId => Json = { case UrlfiedEntityId(url) =>
    Json.fromString(url)
  }
}

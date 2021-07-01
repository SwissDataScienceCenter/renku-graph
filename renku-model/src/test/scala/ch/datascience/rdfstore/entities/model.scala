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

import cats.Show
import cats.syntax.all._
import ch.datascience.graph.model.views.UrlResourceRenderer
import ch.datascience.tinytypes.constraints._
import ch.datascience.tinytypes._
import io.circe.Json
import io.renku.jsonld.EntityId

import java.time.Instant

sealed trait Location extends Any with RelativePathTinyType
object Location {

  final class File private (val value: String) extends AnyVal with Location
  object File extends TinyTypeFactory[File](new File(_)) with RelativePath {
    def apply(folder: Location.Folder, filename: String): Location.File = Location.File(s"$folder/$filename")
  }

  final class Folder private (val value: String) extends AnyVal with Location
  object Folder extends TinyTypeFactory[Folder](new Folder(_)) with RelativePath
}

final class InvalidationTime private (val value: Instant) extends AnyVal with InstantTinyType
object InvalidationTime extends TinyTypeFactory[InvalidationTime](new InvalidationTime(_)) with BoundedInstant {
  import java.time.temporal.ChronoUnit.HOURS
  protected[this] override def maybeMax: Option[Instant] = now.plus(2, HOURS).some
}

final case class UrlfiedEntityId(value: String) extends EntityId with StringTinyType {
  override type Value = String
  override lazy val asJson:    Json         = Json.fromString(value)
  override lazy val valueShow: Show[String] = Show[String](_ => value)
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

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

package ch.datascience.graph.model.testentities

import cats.Show
import ch.datascience.graph.model.views.UrlResourceRenderer
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints._
import io.circe.Json
import io.renku.jsonld.EntityId

final case class UrlfiedEntityId(value: String) extends EntityId with UrlTinyType {
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

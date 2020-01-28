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

package io.renku.jsonld

import io.circe.{Encoder, Json}
import io.renku.jsonld.JsonLD.JsonLDEntity

final case class Reverse(properties: List[(Property, JsonLDEntity)]) extends Product with Serializable

object Reverse {

  def of(first: (Property, JsonLDEntity), other: (Property, JsonLDEntity)*): Reverse =
    Reverse((first +: other).toList)

  lazy val empty: Reverse = Reverse(Nil)

  implicit val jsonEncoder: Encoder[Reverse] = Encoder.instance {
    case Reverse(Nil)                  => Json.Null
    case Reverse((prop, value) +: Nil) => Json.obj(prop.url -> value.toJson)
    case Reverse(props)                => Json.arr(props.map { case (prop, value) => Json.obj(prop.url -> value.toJson) }: _*)
  }
}

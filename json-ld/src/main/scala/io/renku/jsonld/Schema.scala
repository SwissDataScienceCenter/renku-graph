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

abstract class Schema(value: String, separator: String = "/") extends Product with Serializable {
  def /(name: String): Property = Property(s"$value$separator$name")
}

final case class Property(url: String) {
  override lazy val toString: String = url
}

object Schema {

  def from(baseUrl: String): Schema = StandardSchema(baseUrl)

  private[jsonld] final case class StandardSchema(value: String) extends Schema(value)
}

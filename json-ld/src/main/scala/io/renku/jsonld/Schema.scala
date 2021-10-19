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

package io.renku.jsonld

import cats.Show

abstract class Schema(url: String, separator: String) extends Product with Serializable {
  def /(name: String):        Property = Property(s"$url$separator$name")
  def /(name: Number):        Property = Property(s"$url$separator$name")
  def asPrefix(name: String): String   = s"PREFIX $name: <$url$separator>"

  override lazy val toString: String = s"$url$separator"
}

final case class Property(url: String) {
  override lazy val toString: String = url
}

object Property {
  implicit val show: Show[Property] = Show.show(_.url)
}

object Schema {

  def from(baseUrl: String):                    Schema = SlashSeparatorSchema(baseUrl)
  def from(baseUrl: String, separator: String): Schema = CustomSeparatorSchema(baseUrl, separator)

  private[jsonld] final case class SlashSeparatorSchema(value: String) extends Schema(value, separator = "/")
  private[jsonld] final case class CustomSeparatorSchema(value: String, separator: String)
      extends Schema(value, separator)
}

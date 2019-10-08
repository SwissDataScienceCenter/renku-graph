/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.tinytypes.constraints

import java.net.URL

import ch.datascience.tinytypes._

import scala.language.implicitConversions
import scala.util.Try

trait Url extends Constraints[String] {
  addConstraint(
    check   = url => Try(new URL(url)).isSuccess,
    message = (url: String) => s"Cannot instantiate $typeName with '$url'"
  )
}

trait UrlOps[T <: StringTinyType] {
  self: TinyTypeFactory[T] with Url =>

  import ch.datascience.http.client.UrlEncoder._

  case class UrlWithQueryParam(value: T) extends TinyType { type V = T }

  implicit class UrlOps(url: T) {

    def /[TT <: TinyType](value: TT)(implicit converter: TT => List[PathSegment]): T =
      apply(s"$url/${converter(value).mkString("/")}")

    def /(value: String): T = apply(s"$url/${urlEncode(value)}")

    def ?(keyAndValue: (String, String)): UrlWithQueryParam = keyAndValue match {
      case (key, value) => UrlWithQueryParam(apply(s"$url?$key=${urlEncode(value)}"))
    }
  }

  implicit class UrlWithQueryParamOps(url: UrlWithQueryParam) {
    def &(keyAndValue: (String, String)): UrlWithQueryParam = keyAndValue match {
      case (key, value) => UrlWithQueryParam(apply(s"$url&$key=${urlEncode(value)}"))
    }
  }

  implicit def toUrl(url: UrlWithQueryParam): T = url.value
}

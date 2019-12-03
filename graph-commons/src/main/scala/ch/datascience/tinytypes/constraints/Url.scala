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

    def ?[Value](keyAndValue: (String, Value))(implicit convert: Value => QueryParamValue): UrlWithQueryParam =
      keyAndValue match {
        case (key, value) =>
          UrlWithQueryParam(
            apply(
              if (url.toString contains s"$key=")
                url.toString.replaceAll(s"($key=[\\w\\d%\\+]*)", s"$key=${convert(value)}")
              else if (url.toString contains "?")
                s"$url&$key=${convert(value)}"
              else
                s"$url?$key=${convert(value)}"
            )
          )
      }
  }

  implicit class UrlWithQueryParamOps(url: UrlWithQueryParam) {

    def &[Value](keyAndValue: (String, Value))(implicit convert: Value => QueryParamValue): UrlWithQueryParam =
      keyAndValue match {
        case (key, value) => add(key, value)
      }

    def &&[Value](
        keyAndValue:    (String, Option[Value])
    )(implicit convert: Value => QueryParamValue): UrlWithQueryParam =
      keyAndValue match {
        case (_, None)          => url
        case (key, Some(value)) => add(key, value)
      }

    private def add[Value](key: String, value: Value)(implicit convert: Value => QueryParamValue) =
      UrlWithQueryParam {
        apply {
          if (url.toString contains s"$key=")
            url.toString.replaceAll(s"($key=[\\w\\d%\\+]*)", s"$key=${convert(value)}")
          else
            s"$url&$key=${convert(value)}"
        }
      }
  }

  case class QueryParamValue(value: String) extends StringTinyType

  implicit val stringParamValue: String => QueryParamValue = v => QueryParamValue(urlEncode(v))
  implicit val intParamValue:    Int => QueryParamValue    = v => QueryParamValue(v.toString)
  implicit def stringTinyTypeParamValue[TT <: StringTinyType]: TT => QueryParamValue =
    v => QueryParamValue(urlEncode(v.value))
  implicit def intTinyTypeParamValue[TT <: IntTinyType]: TT => QueryParamValue =
    v => QueryParamValue(v.toString)

  implicit def toUrl(url: UrlWithQueryParam): T = url.value
}

trait BaseUrl[SourceType <: StringTinyType, DestinationType <: StringTinyType] {
  self: TinyTypeFactory[SourceType] with Url =>

  import ch.datascience.http.client.UrlEncoder._

  implicit class BaseUrlOps(url: SourceType) {
    def /(value: String)(implicit typeFactory: TinyTypeFactory[DestinationType]): DestinationType =
      typeFactory(s"$url/${urlEncode(value)}")
  }
}

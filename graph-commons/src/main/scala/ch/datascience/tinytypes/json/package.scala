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

package ch.datascience.tinytypes

import java.time.Instant

import play.api.libs.json._

import scala.language.implicitConversions

package object json {

  implicit def jsStringReads(jsValue: JsValue): JsResult[String] =
    jsValue
      .validate[String]
      .fold(
        _ => JsError(s"Expected String but got '$jsValue'"),
        value => JsSuccess(value)
      )

  implicit val stringToJson: String => JsValue = JsString

  implicit def jsIntNumberReads(jsValue: JsValue): JsResult[Int] =
    jsValue
      .validate[Int]
      .fold(
        _ => JsError(s"Expected Int but got '$jsValue'"),
        value => JsSuccess(value)
      )

  implicit val intToJson: Int => JsValue = JsNumber(_)

  implicit def jsInstantReads(jsValue: JsValue): JsResult[Instant] =
    jsValue
      .validate[Instant]
      .fold(
        _ => JsError(s"Expected Instant but got '$jsValue'"),
        value => JsSuccess(value)
      )

  implicit val instantToJson: Instant => JsValue =
    instant => implicitly[Writes[Instant]].writes(instant)
}

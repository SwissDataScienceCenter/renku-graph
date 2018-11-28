/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.graph

import play.api.libs.json.{ JsPath, OFormat }
import play.api.libs.functional.syntax._

case class User(
    username: String,
    email:    String,
    gitlabId: Option[Int]
)

object User {
  implicit lazy val format: OFormat[User] = (
    ( JsPath \ 'username ).format[String] and
    ( JsPath \ 'email ).format[String] and
    ( JsPath \ 'gitlabId ).formatNullable[Int]
  )( User.apply, unlift( User.unapply ) )
}

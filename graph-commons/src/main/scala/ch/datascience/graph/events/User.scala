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

package ch.datascience.graph.events

import ch.datascience.tinytypes.constraints.{NonBlank, NonNegative}
import ch.datascience.tinytypes.json._
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import play.api.libs.json.Format

case class PushUser(
    userId:   UserId,
    username: Username,
    email:    Email
)

case class User(
    username: Username,
    email:    Email
)

class UserId private (val value: Int) extends AnyVal with TinyType[Int]
object UserId extends TinyTypeFactory[Int, UserId](new UserId(_)) with NonNegative {

  implicit lazy val userIdFormat: Format[UserId] = TinyTypeFormat(UserId.apply)
}

class Username private (val value: String) extends AnyVal with TinyType[String]
object Username extends TinyTypeFactory[String, Username](new Username(_)) with NonBlank {

  implicit lazy val usernameFormat: Format[Username] = TinyTypeFormat(Username.apply)
}

class Email private (val value: String) extends AnyVal with TinyType[String]
object Email extends TinyTypeFactory[String, Email](new Email(_)) with NonBlank {

  implicit lazy val emailFormat: Format[Email] = TinyTypeFormat(Email.apply)
}

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

package ch.datascience.http.client

import cats.implicits._
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

case class BasicAuthCredentials(username: BasicAuthUsername, password: BasicAuthPassword)

class BasicAuthUsername private (val value: String) extends AnyVal with TinyType[String]
object BasicAuthUsername extends TinyTypeFactory[String, BasicAuthUsername](new BasicAuthUsername(_)) with NonBlank

class BasicAuthPassword private (val value: String) extends AnyVal with TinyType[String]
object BasicAuthPassword extends TinyTypeFactory[String, BasicAuthPassword](new BasicAuthPassword(_)) with NonBlank

object BasicAuthConfigReaders {

  implicit val usernameReader: ConfigReader[BasicAuthUsername] =
    ConfigReader.fromString[BasicAuthUsername] { value =>
      BasicAuthUsername
        .from(value)
        .leftMap(exception => CannotConvert(value, BasicAuthUsername.getClass.toString, exception.getMessage))
    }

  implicit val passwordReader: ConfigReader[BasicAuthPassword] =
    ConfigReader.fromString[BasicAuthPassword] { value =>
      BasicAuthPassword
        .from(value)
        .leftMap(exception => CannotConvert(value, BasicAuthPassword.getClass.toString, exception.getMessage))
    }
}

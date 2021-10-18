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

package io.renku.http.client

import io.renku.tinytypes.constraints.NonBlank
import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

case class BasicAuthCredentials(username: BasicAuthUsername, password: BasicAuthPassword)

class BasicAuthUsername private (val value: String) extends AnyVal with StringTinyType
object BasicAuthUsername extends TinyTypeFactory[BasicAuthUsername](new BasicAuthUsername(_)) with NonBlank

class BasicAuthPassword private (val value: String) extends AnyVal with StringTinyType
object BasicAuthPassword extends TinyTypeFactory[BasicAuthPassword](new BasicAuthPassword(_)) with NonBlank

object BasicAuthConfigReaders {

  import io.renku.config.ConfigLoader._
  import pureconfig.ConfigReader

  implicit val usernameReader: ConfigReader[BasicAuthUsername] = stringTinyTypeReader(BasicAuthUsername)
  implicit val passwordReader: ConfigReader[BasicAuthPassword] = stringTinyTypeReader(BasicAuthPassword)
}

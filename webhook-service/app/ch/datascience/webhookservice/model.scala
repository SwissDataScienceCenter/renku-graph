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

package ch.datascience.webhookservice

import ch.datascience.tinytypes.StringValue
import ch.datascience.tinytypes.constraints.NonBlank

case class FilePath( value: String ) extends StringValue with NonBlank {
  verify( !value.startsWith( "/" ), s"'$value' is not a valid $typeName" )
}

trait GitSha extends StringValue with NonBlank {

  import GitSha.validationRegex

  verify( value.matches( validationRegex ), s"'$value' is not a valid Git sha" )
}

object GitSha {
  private val validationRegex: String = "[0-9a-f]{5,40}"
}

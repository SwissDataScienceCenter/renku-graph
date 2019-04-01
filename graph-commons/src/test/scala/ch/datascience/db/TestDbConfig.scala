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

package ch.datascience.db

import ch.datascience.db.DBConfigProvider.DBConfig
import ch.datascience.db.DBConfigProvider.DBConfig.Url
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import eu.timepit.refined.api.RefType
import eu.timepit.refined.auto._

object TestDbConfig {

  def newDbConfig[TargetDb]: DBConfig[TargetDb] = {
    val dbName = nonEmptyStrings().map(suffix => s"db_$suffix").generateOne
    DBConfig[TargetDb](
      driver         = "org.h2.Driver",
      url            = toUrl(s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1;MODE=PostgreSQL"),
      user           = "user",
      pass           = "",
      connectionPool = 20
    )
  }

  private def toUrl(value: String): Url =
    RefType
      .applyRef[Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid db driver url value"))
}

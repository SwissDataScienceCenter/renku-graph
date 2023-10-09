/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.db

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.db.DBConfigProvider.DBConfig
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings

object TestDbConfig {

  def create[TargetDb]: DBConfig[TargetDb] = DBConfig[TargetDb](
    name = Refined.unsafeApply(nonEmptyStrings().map(suffix => s"db_$suffix").generateOne),
    host = "localhost",
    port = 5432,
    user = "user",
    pass = "test",
    connectionPool = 20
  )
}

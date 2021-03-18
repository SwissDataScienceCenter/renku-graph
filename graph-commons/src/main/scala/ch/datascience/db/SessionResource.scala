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

package ch.datascience.db

import cats.effect._
import ch.datascience.db.DBConfigProvider.DBConfig
import natchez.Trace
import skunk.Session

import scala.language.postfixOps

class SessionResource[Interpretation[_], TargetDB](resource: Resource[Interpretation, Session[Interpretation]]) {
  def use[G[x] >: Interpretation[x], B](f: Session[Interpretation] => G[B])(implicit F: BracketThrow[G]): G[B] =
    resource.use(f)
}

object SessionPoolResource {
  def apply[Interpretation[_]: Concurrent: ContextShift: Trace, TargetDB](
      dbConfig: DBConfig[TargetDB]
  )(implicit
      async: Async[Interpretation]
  ): Resource[Interpretation, SessionResource[Interpretation, TargetDB]] =
    Session
      .pooled(
        host = dbConfig.host.value,
        port = 5432,
        user = dbConfig.user.value,
        database = dbConfig.name.value,
        password = Some(dbConfig.pass),
        max = dbConfig.connectionPool.value,
        readTimeout = dbConfig.maxLifetime,
        writeTimeout = dbConfig.maxLifetime
      )
      .map(new SessionResource(_))
}

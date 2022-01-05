/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.data.Kleisli
import cats.effect._
import cats.effect.std.Console
import fs2.io.net.Network
import io.renku.db.DBConfigProvider.DBConfig
import natchez.Trace
import skunk.{Session, Transaction}

class SessionResource[F[_]: MonadCancelThrow, TargetDB](
    resource: Resource[F, Session[F]]
) {

  def useK[ResultType](
      query: Kleisli[F, Session[F], ResultType]
  ): F[ResultType] = resource.use(query.run)

  def useWithTransactionK[ResultType](
      query: Kleisli[F, (Transaction[F], Session[F]), ResultType]
  ): F[ResultType] = resource.use { session =>
    session.transaction.use(transaction => query.run((transaction, session)))
  }
}

object SessionPoolResource {
  def apply[F[_]: Concurrent: Trace: Network: Console, TargetDB](
      dbConfig: DBConfig[TargetDB]
  ): Resource[F, SessionResource[F, TargetDB]] =
    Session
      .pooled(
        host = dbConfig.host.value,
        port = dbConfig.port.value,
        user = dbConfig.user.value,
        database = dbConfig.name.value,
        password = Some(dbConfig.pass.value),
        max = dbConfig.connectionPool.value
      )
      .map(new SessionResource(_))
}

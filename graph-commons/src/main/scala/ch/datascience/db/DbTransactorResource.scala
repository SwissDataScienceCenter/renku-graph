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

import cats.effect._
import ch.datascience.db.DBConfigProvider.DBConfig
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class DbTransactorResource[Interpretation[_], TargetDB](
    dbConfig:     DBConfig[TargetDB]
)(implicit async: Async[Interpretation], cs: ContextShift[Interpretation]) {

  import ExecutionContexts._
  import HikariTransactor._

  def use[B](
      runWithTransactor: DbTransactor[Interpretation, TargetDB] => Interpretation[B]
  )(implicit ME:         Bracket[Interpretation, Throwable]): Interpretation[B] =
    transactorResource.use { transactor =>
      runWithTransactor(new DbTransactor[Interpretation, TargetDB](transactor))
    }

  private lazy val transactorResource: Resource[Interpretation, HikariTransactor[Interpretation]] =
    for {
      connectionsThreadPool  <- fixedThreadPool[Interpretation](dbConfig.connectionPool.value)
      transactionsThreadPool <- cachedThreadPool[Interpretation]
      transactor             <- createHikariTransactor(connectionsThreadPool, transactionsThreadPool)
    } yield transactor

  private def createHikariTransactor(connectionsThreadPool:  ExecutionContext,
                                     transactionsThreadPool: ExecutionContext) =
    for {
      _          <- Resource.liftF(Async[Interpretation].delay(Class.forName(dbConfig.driver.value)))
      transactor <- initial[Interpretation](connectionsThreadPool, transactionsThreadPool)
      _ <- Resource.liftF {
            transactor.configure { dataSource =>
              Async[Interpretation].delay {
                dataSource setJdbcUrl dbConfig.url.value
                dataSource setUsername dbConfig.user.value
                dataSource setPassword dbConfig.pass
                dataSource setMaxLifetime dbConfig.maxLifetime.toMillis
              }
            }
          }
    } yield transactor
}

object DbTransactorResource {
  def apply[Interpretation[_], TargetDB](
      dbConfig:     DBConfig[TargetDB]
  )(implicit async: Async[Interpretation],
    cs:             ContextShift[Interpretation]): DbTransactorResource[Interpretation, TargetDB] =
    new DbTransactorResource[Interpretation, TargetDB](dbConfig)
}

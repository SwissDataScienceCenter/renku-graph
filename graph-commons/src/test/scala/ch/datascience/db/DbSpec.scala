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

import cats.effect.{ContextShift, IO}
import ch.datascience.db.DBConfigProvider.DBConfig
import ch.datascience.db.DBConfigProvider.DBConfig.Url
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.orchestration.Provider
import doobie.util.transactor.Transactor.Aux
import eu.timepit.refined.api.RefType
import eu.timepit.refined.auto._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, TestSuite}

import scala.concurrent.ExecutionContext.Implicits.global

trait DbSpec extends BeforeAndAfterAll with BeforeAndAfter {
  self: TestSuite =>

  protected implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  protected lazy val dbName: String = nonEmptyStrings().map(suffix => s"db_$suffix").generateOne
  private val h2ConfigProvider = new Provider[IO, DBConfig] {
    override def get() = IO.pure(
      DBConfig(
        driver = "org.h2.Driver",
        url    = toUrl(s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1;MODE=PostgreSQL"),
        user   = "user",
        pass   = ""
      )
    )
  }
  protected lazy val transactorProvider: TransactorProvider[IO] = new TransactorProvider(h2ConfigProvider)
  protected lazy val transactor:         Aux[IO, Unit]          = transactorProvider.transactor.unsafeRunSync()

  protected def initDb(transactor:           Aux[IO, Unit]): Unit
  protected def prepareDbForTest(transactor: Aux[IO, Unit]): Unit

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    initDb(transactor)
  }

  before {
    prepareDbForTest(transactor)
  }

  private def toUrl(value: String): Url =
    RefType
      .applyRef[Url](value)
      .getOrElse(throw new IllegalArgumentException("Invalid db driver url value"))
}

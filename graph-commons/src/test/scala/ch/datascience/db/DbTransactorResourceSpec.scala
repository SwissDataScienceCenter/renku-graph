/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

import DbConfigGenerator._
import cats.effect.{ContextShift, IO}
import com.zaxxer.hikari.HikariDataSource
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class DbTransactorResourceSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "use" should {

    "pass the transactor built with the DBConfig and DataSourceUpdater to the given block" in new TestCase {

      dataSourceUpdater.expects(*)

      transactedBlock.expects(*).returning(IO.unit)

      transactorResource.use(transactedBlock).unsafeRunSync() shouldBe ((): Unit)
    }
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(global)

  private trait TestCase {
    val transactedBlock    = mockFunction[DbTransactor[IO, TestDB], IO[Unit]]
    val dataSourceUpdater  = mockFunction[HikariDataSource, Unit]
    val dbConfig           = TestDbConfig.newDbConfig[TestDB]
    val transactorResource = new DbTransactorResource[IO, TestDB](dbConfig, dataSourceUpdater)
  }
}

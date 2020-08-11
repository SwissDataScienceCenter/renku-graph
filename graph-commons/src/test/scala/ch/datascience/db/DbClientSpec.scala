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

import cats.effect.{ContextShift, IO}
import ch.datascience.db.SqlQuery.Name
import ch.datascience.db.TestDbConfig.newDbConfig
import ch.datascience.metrics.{LabeledHistogram, TestLabeledHistogram}
import doobie.implicits._
import doobie.util.transactor.Transactor
import eu.timepit.refined.auto._
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext.Implicits.global

class DbClientSpec extends AnyWordSpec with should.Matchers {

  "measureExecutionTime" should {

    "execute the query and do nothing if no histogram given" in {
      val result = 1
      new TestDbClient(maybeHistogram = None).executeQuery(expected = result).unsafeRunSync() shouldBe result
    }

    "execute the query and measure execution time with the given histogram" in {

      val histogram = TestLabeledHistogram[SqlQuery.Name]("query_id")

      val dbClient = new TestDbClient(maybeHistogram = Some(histogram))

      val result = 1
      dbClient.executeQuery(expected = result).unsafeRunSync() shouldBe result

      histogram.verifyExecutionTimeMeasured(forLabelValue = dbClient.queryName)
    }
  }
}

private trait TestDB

private class TestDbClient(maybeHistogram: Option[LabeledHistogram[IO, Name]]) extends DbClient(maybeHistogram) {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(global)

  private val dbConfig: DBConfigProvider.DBConfig[TestDB] = newDbConfig[TestDB]

  private val transactor: DbTransactor[IO, TestDB] = DbTransactor[IO, TestDB](
    Transactor.fromDriverManager[IO](
      dbConfig.driver.value,
      dbConfig.url.value,
      dbConfig.user.value,
      dbConfig.pass
    )
  )

  val queryName: SqlQuery.Name = "some_id"

  private def query(expected: Int) = SqlQuery(
    sql"""select $expected;""".query[Int].unique,
    queryName
  )

  def executeQuery(expected: Int) =
    measureExecutionTime[Int] {
      query(expected)
    } transact transactor.get
}

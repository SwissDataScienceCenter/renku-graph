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

import cats.data.Kleisli
import cats.effect.{ContextShift, IO, Resource}
import ch.datascience.db.SqlQuery.Name
import ch.datascience.db.TestDbConfig.newDbConfig
import ch.datascience.metrics.{LabeledHistogram, TestLabeledHistogram}
import eu.timepit.refined.auto._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import skunk._
import skunk.implicits._
import skunk.codec.all._
import natchez.Trace.Implicits.noop

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

  private val transactorResource: Resource[IO, SessionResource[IO, TestDB]] = SessionPoolResource[IO, TestDB](dbConfig)

  val queryName: SqlQuery.Name = "some_id"

  private def query(expected: Int) = SqlQuery[IO, Int](Kleisli { session =>
                                                         val query: Query[Int, Int] =
                                                           sql"""select $int4;""".query(int4)
                                                         session.prepare(query).use { pq =>
                                                           pq.unique(expected)
                                                         }
                                                       },
                                                       queryName
  )

  def executeQuery(expected: Int) = transactorResource.use {
    _.use { implicit session =>
      measureExecutionTime[Int] {
        query(expected)
      }
    }
  }
}

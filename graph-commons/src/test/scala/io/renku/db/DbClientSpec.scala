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

import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.db.syntax.QueryDef
import io.renku.metrics.LabeledHistogram
import io.renku.testtools.CustomAsyncIOSpec
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import skunk._
import skunk.codec.all._
import skunk.implicits._

import scala.concurrent.duration.FiniteDuration

class DbClientSpec
    extends AsyncWordSpec
    with CommonsPostgresSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with AsyncMockFactory {

  "measureExecutionTime(SqlStatement)" should {

    "execute the query and do nothing if no histogram given" in {
      val result = 1
      new TestDbClient(maybeHistogram = None)
        .executeQuery(query(queryName, returning = result))
        .asserting(_ shouldBe result)
    }

    "execute the query and measure execution time with the given histogram" in {

      val histogram = mock[LabeledHistogram[IO]]
      val dbClient  = new TestDbClient(histogram.some)

      (histogram
        .observe(_: String, _: FiniteDuration))
        .expects(queryName.value, *)
        .returning(().pure[IO])

      val result = 1

      dbClient.executeQuery(query(queryName, returning = result)).asserting(_ shouldBe result)
    }
  }

  "measureExecutionTime(Kleisli)" should {

    "execute the query and do nothing if no histogram given" in {
      val result = 1
      new TestDbClient(maybeHistogram = None)
        .executeQuery(queryName.value, QueryDef[IO, Int](query(queryName, returning = result).queryExecution.run))
        .asserting(_ shouldBe result)
    }

    "execute the query and measure execution time with the given histogram" in {

      val histogram = mock[LabeledHistogram[IO]]
      val dbClient  = new TestDbClient(maybeHistogram = Some(histogram))
      (histogram
        .observe(_: String, _: FiniteDuration))
        .expects(queryName.value, *)
        .returning(().pure[IO])

      val result = 1

      dbClient
        .executeQuery(queryName, QueryDef[IO, Int](query(queryName, returning = result).queryExecution.run))
        .asserting(_ shouldBe result)
    }
  }

  private lazy val queryName: SqlStatement.Name = "some_id"

  private def query(name: SqlStatement.Name, returning: Int) = SqlStatement[IO, Int](
    QueryDef[IO, Int] { session =>
      val query: Query[Int, Int] = sql"""select $int4;""".query(int4)
      session.prepare(query).flatMap(_.unique(returning))
    },
    name
  )

  private class TestDbClient(maybeHistogram: Option[LabeledHistogram[IO]]) extends DbClient(maybeHistogram) {

    def executeQuery(query: SqlStatement[IO, Int]): IO[Int] =
      (testDBResource >>= sessionResource).use {
        measureExecutionTime[Int](query)(_)
      }

    def executeQuery(name: String, query: Kleisli[IO, Session[IO], Int]): IO[Int] =
      (testDBResource >>= sessionResource).use {
        measureExecutionTime[Int](name, query)(_)
      }
  }
}

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
import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import eu.timepit.refined.auto._
import io.renku.db.TestDbConfig.newDbConfig
import io.renku.metrics.{LabeledHistogram, TestLabeledHistogram}
import io.renku.testtools.IOSpec
import natchez.Trace.Implicits.noop
import org.scalatest.Suite
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.testcontainers.utility.DockerImageName
import skunk._
import skunk.codec.all._
import skunk.implicits._

class DbClientSpec extends AnyWordSpec with IOSpec with should.Matchers with ContainerTestDb {

  "measureExecutionTime" should {

    "execute the query and do nothing if no histogram given" in {
      val result = 1
      new TestDbClient(maybeHistogram = None)
        .executeQuery(expected = result)(sessionPoolResource)
        .unsafeRunSync() shouldBe result
    }

    "execute the query and measure execution time with the given histogram" in {

      val histogram = TestLabeledHistogram[SqlStatement.Name]("query_id")

      val dbClient = new TestDbClient(maybeHistogram = Some(histogram))

      val result = 1
      dbClient.executeQuery(expected = result)(sessionPoolResource).unsafeRunSync() shouldBe result

      histogram.verifyExecutionTimeMeasured(forLabelValue = dbClient.queryName)
    }
  }
}

private class TestDbClient(maybeHistogram: Option[LabeledHistogram[IO]]) extends DbClient(maybeHistogram) {
  val queryName: SqlStatement.Name = "some_id"

  private def query(expected: Int) = SqlStatement[IO, Int](Kleisli { session =>
                                                             val query: Query[Int, Int] =
                                                               sql"""select $int4;""".query(int4)
                                                             session.prepare(query).use { pq =>
                                                               pq.unique(expected)
                                                             }
                                                           },
                                                           queryName
  )

  def executeQuery(expected: Int)(sessionPoolResource: Resource[IO, Resource[IO, Session[IO]]]): IO[Int] =
    sessionPoolResource.use {
      _.use(session => measureExecutionTime[Int](query(expected))(session))
    }
}

trait ContainerTestDb extends ForAllTestContainer {
  self: Suite =>

  private trait TestDB

  private val dbConfig: DBConfigProvider.DBConfig[TestDB] = newDbConfig[TestDB]

  override val container: PostgreSQLContainer = PostgreSQLContainer(
    dockerImageNameOverride = DockerImageName.parse("postgres:12.8-alpine"),
    databaseName = dbConfig.name.value,
    username = dbConfig.user.value,
    password = dbConfig.pass
  )

  lazy val sessionPoolResource: Resource[IO, Resource[IO, Session[IO]]] = Session.pooled(
    host = container.host,
    port = container.container.getMappedPort(dbConfig.port),
    user = dbConfig.user.value,
    database = dbConfig.name.value,
    password = Some(dbConfig.pass),
    max = dbConfig.connectionPool.value
  )
}

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

import DbConfigGenerator._
import ch.datascience.db.DBConfigProvider.DBConfig._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import com.zaxxer.hikari.HikariDataSource
import eu.timepit.refined.api.RefType.applyRef
import org.scalacheck.Gen.choose
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._
import scala.language.postfixOps

class DataSourceUpdaterSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "DataSourceUpdater" should {

    "set JDBC url on the data source" in new TestCase {
      updater(dataSource)
      (dataSource.setJdbcUrl(_: String)) verify dbConfig.url.value
    }

    "set username on the data source" in new TestCase {
      updater(dataSource)
      (dataSource.setUsername(_: String)) verify dbConfig.user.value
    }

    "set password on the data source" in new TestCase {
      updater(dataSource)
      (dataSource.setPassword(_: String)) verify dbConfig.pass
    }

    "set maximumPoolSize on the data source" in new TestCase {
      updater(dataSource)
      (dataSource.setMaximumPoolSize(_: Int)) verify dbConfig.connectionPool.value
    }

    "set minimumIdle on the data source as 75% of connection pool" in new TestCase {
      new DataSourceUpdater(
        dbConfig.copy(connectionPool = applyRef[ConnectionPool](choose(1, 3).generateOne).getOrError)
      )(dataSource)
      (dataSource.setMinimumIdle(_: Int)).verify(*).never()
    }

    "do not set minimumIdle on the data source if 75% of connection pool is the same as connection pool" in new TestCase {
      updater(dataSource)
      (dataSource.setMinimumIdle(_: Int)) verify (dbConfig.connectionPool.value * 0.75).ceil.toInt
    }

    "set maxLifetime on the data source" in new TestCase {
      updater(dataSource)
      (dataSource.setMaxLifetime(_: Long)) verify dbConfig.maxLifetime.toMillis
    }

    "set idleTimeout on the data source as (maxLifetime - 30s) if maxLifetime > 30s" in new TestCase {
      val maxLifetime = durations(max = 10 minutes).generateOne
      new DataSourceUpdater(dbConfig.copy(maxLifetime = maxLifetime))(dataSource)
      (dataSource.setIdleTimeout(_: Long)) verify (maxLifetime / 2).toMillis
    }

  }

  private trait TestCase {
    val dbConfig = dbConfigs.generateOne.copy[TestDB](
      connectionPool = applyRef[ConnectionPool](choose(4, 1000).generateOne).getOrError
    )
    val dataSource = stub[HikariDataSource]

    val updater = new DataSourceUpdater(dbConfig)
  }
}

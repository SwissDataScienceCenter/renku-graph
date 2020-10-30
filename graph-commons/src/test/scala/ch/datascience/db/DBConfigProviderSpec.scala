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

import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.db.DBConfigProvider.DBConfig._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.RefType
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class DBConfigProviderSpec extends AnyWordSpec with should.Matchers {

  "get" should {

    "return db config read from the configuration" in new TestCase {
      val host           = hosts.generateOne
      val user           = nonEmptyStrings().generateOne
      val password       = nonEmptyStrings().generateOne
      val connectionPool = positiveInts().generateOne
      val maxLifetime    = durations(max = 30 minutes).generateOne

      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"               -> driver.value,
            "db-url-template"         -> urlTemplate.value,
            "db-host"                 -> host.value,
            "db-user"                 -> user,
            "db-pass"                 -> password,
            "connection-pool"         -> connectionPool.value,
            "max-connection-lifetime" -> maxLifetime.toString()
          ).asJava
        ).asJava
      )

      val Success(dbConfig) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      dbConfig.driver         shouldBe driver
      dbConfig.url.value      shouldBe urlTemplate.value.replace("$host", host.value).replace("$dbName", dbName.value)
      dbConfig.user.value     shouldBe user
      dbConfig.pass           shouldBe password
      dbConfig.connectionPool shouldBe connectionPool
      dbConfig.maxLifetime    shouldBe maxLifetime
    }

    "fail if there is no db config namespace in the config" in new TestCase {
      val Failure(exception) =
        new DBConfigProvider[Try, TestDB](namespace, dbName, ConfigFactory.empty()).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-driver' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"               -> "",
            "db-url-template"         -> nonEmptyStrings().generateOne,
            "db-host"                 -> hosts.generateOne.value,
            "db-user"                 -> nonEmptyStrings().generateOne,
            "db-pass"                 -> nonEmptyStrings().generateOne,
            "connection-pool"         -> positiveInts().generateOne.value,
            "max-connection-lifetime" -> durations(max = 30 minutes).generateOne.toString()
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-host' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"               -> nonEmptyStrings().generateOne,
            "db-url-template"         -> nonEmptyStrings().generateOne,
            "db-host"                 -> "",
            "db-user"                 -> nonEmptyStrings().generateOne,
            "db-pass"                 -> nonEmptyStrings().generateOne,
            "connection-pool"         -> positiveInts().generateOne.value,
            "max-connection-lifetime" -> durations(max = 30 minutes).generateOne.toString()
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-user' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"               -> nonEmptyStrings().generateOne,
            "db-url-template"         -> nonEmptyStrings().generateOne,
            "db-host"                 -> hosts.generateOne.value,
            "db-user"                 -> "",
            "db-pass"                 -> nonEmptyStrings().generateOne,
            "connection-pool"         -> positiveInts().generateOne.value,
            "max-connection-lifetime" -> durations(max = 30 minutes).generateOne.toString()
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-pass' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"               -> nonEmptyStrings().generateOne,
            "db-url-template"         -> nonEmptyStrings().generateOne,
            "db-host"                 -> hosts.generateOne.value,
            "db-user"                 -> nonEmptyStrings().generateOne,
            "connection-pool"         -> positiveInts().generateOne.value,
            "max-connection-lifetime" -> durations(max = 30 minutes).generateOne.toString()
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.connection-pool' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"               -> nonEmptyStrings().generateOne,
            "db-url-template"         -> nonEmptyStrings().generateOne,
            "db-host"                 -> hosts.generateOne.value,
            "db-user"                 -> nonEmptyStrings().generateOne,
            "db-pass"                 -> nonEmptyStrings().generateOne,
            "max-connection-lifetime" -> durations(max = 30 minutes).generateOne.toString()
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.max-connection-lifetime' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"       -> nonEmptyStrings().generateOne,
            "db-url-template" -> nonEmptyStrings().generateOne,
            "db-host"         -> hosts.generateOne.value,
            "db-user"         -> nonEmptyStrings().generateOne,
            "db-pass"         -> nonEmptyStrings().generateOne,
            "connection-pool" -> positiveInts().generateOne.value
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-url-template' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-driver"               -> nonEmptyStrings().generateOne,
            "db-url-template"         -> "",
            "db-host"                 -> hosts.generateOne.value,
            "db-user"                 -> nonEmptyStrings().generateOne,
            "db-pass"                 -> nonEmptyStrings().generateOne,
            "connection-pool"         -> positiveInts().generateOne.value,
            "max-connection-lifetime" -> durations(max = 30 minutes).generateOne.toString()
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }
  }

  private trait TestCase {
    sealed trait TestDB

    val namespace = nonEmptyStrings().generateOne

    val driver = RefType
      .applyRef[Driver](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid driver value"))
    val dbName = RefType
      .applyRef[DbName](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid dbName value"))
    val urlTemplate = RefType
      .applyRef[UrlTemplate](s"${nonEmptyStrings().generateOne}/$$host/$$dbName")
      .getOrElse(throw new IllegalArgumentException("Invalid urlPrefix value"))

    val hosts: Gen[Host] = for {
      hostname <- nonEmptyStrings()
      port     <- positiveInts()
    } yield RefType
      .applyRef[Host](s"$hostname:$port")
      .getOrElse(throw new IllegalArgumentException("Invalid host` value"))
  }
}

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

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.db.DBConfigProvider.DBConfig._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.RefType
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class DBConfigProviderSpec extends WordSpec {

  "get" should {

    "return db config read from the configuration" in new TestCase {
      val host           = hosts.generateOne
      val user           = nonEmptyStrings().generateOne
      val password       = nonEmptyStrings().generateOne
      val connectionPool = positiveInts().generateOne

      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-host"         -> host.value,
            "db-user"         -> user,
            "db-pass"         -> password,
            "connection-pool" -> connectionPool
          ).asJava
        ).asJava
      )

      val Success(dbConfig) = new DBConfigProvider[Try, TestDB](namespace, driver, dbName, urlPrefix, config).get()

      dbConfig.driver               shouldBe driver
      dbConfig.url.value            shouldBe s"$urlPrefix://$host/$dbName"
      dbConfig.user.value           shouldBe user
      dbConfig.pass                 shouldBe password
      dbConfig.connectionPool.value shouldBe connectionPool
    }

    "fail if there is no db config namespace in the config" in new TestCase {
      val Failure(exception) =
        new DBConfigProvider[Try, TestDB](namespace, driver, dbName, urlPrefix, ConfigFactory.empty()).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-host' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-host"         -> "",
            "db-user"         -> nonEmptyStrings().generateOne,
            "db-pass"         -> nonEmptyStrings().generateOne,
            "connection-pool" -> positiveInts().generateOne
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, driver, dbName, urlPrefix, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-user' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-host"         -> hosts.generateOne.value,
            "db-user"         -> "",
            "db-pass"         -> nonEmptyStrings().generateOne,
            "connection-pool" -> positiveInts().generateOne
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, driver, dbName, urlPrefix, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-pass' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-host"         -> hosts.generateOne.value,
            "db-user"         -> nonEmptyStrings().generateOne,
            "connection-pool" -> positiveInts().generateOne
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, driver, dbName, urlPrefix, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.connection-pool' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-host" -> hosts.generateOne.value,
            "db-user" -> nonEmptyStrings().generateOne,
            "db-pass" -> nonEmptyStrings().generateOne
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, driver, dbName, urlPrefix, config).get()

      exception shouldBe a[ConfigLoadingException]
    }
  }

  private implicit val context: MonadError[Try, Throwable] = MonadError[Try, Throwable]

  private trait TestCase {
    sealed trait TestDB

    val namespace = nonEmptyStrings().generateOne

    val driver = RefType
      .applyRef[Driver](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid driver value"))
    val dbName = RefType
      .applyRef[DbName](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid dbName value"))
    val urlPrefix = RefType
      .applyRef[UrlPrefix](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid urlPrefix value"))

    val hosts: Gen[Host] = for {
      hostname <- nonEmptyStrings()
      port     <- positiveInts()
    } yield
      RefType
        .applyRef[Host](s"$hostname:$port")
        .getOrElse(throw new IllegalArgumentException("Invalid host` value"))
  }
}

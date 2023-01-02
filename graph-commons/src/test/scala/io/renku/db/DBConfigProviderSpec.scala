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

import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.RefType
import eu.timepit.refined.api.RefType.refinedRefType
import io.renku.config.ConfigLoader.ConfigLoadingException
import io.renku.db.DBConfigProvider.DBConfig._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class DBConfigProviderSpec extends AnyWordSpec with should.Matchers {

  "get" should {

    "return db config read from the configuration" in new TestCase {
      val host           = hosts.generateOne
      val user           = nonEmptyStrings().generateOne
      val port           = ports.generateOne
      val password       = nonEmptyStrings().generateOne
      val connectionPool = positiveInts().generateOne

      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map[String, Any](
            "db-host"         -> host.value,
            "db-port"         -> port.value,
            "db-user"         -> user,
            "db-pass"         -> password,
            "connection-pool" -> connectionPool.value
          ).asJava
        ).asJava
      )

      val Success(dbConfig) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      dbConfig.user.value     shouldBe user
      dbConfig.host           shouldBe host
      dbConfig.port           shouldBe port
      dbConfig.pass.value     shouldBe password
      dbConfig.connectionPool shouldBe connectionPool
    }

    "fail if there is no db config namespace in the config" in new TestCase {
      val Failure(exception) =
        new DBConfigProvider[Try, TestDB](namespace, dbName, ConfigFactory.empty()).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-host' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map[String, Any](
            "db-host"         -> "",
            "db-port"         -> positiveInts().generateOne.value,
            "db-user"         -> nonEmptyStrings().generateOne,
            "db-pass"         -> nonEmptyStrings().generateOne,
            "connection-pool" -> positiveInts().generateOne.value
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-port' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map[String, Any](
            "db-host"         -> nonEmptyStrings().generateOne,
            "db-user"         -> nonEmptyStrings().generateOne,
            "db-pass"         -> nonEmptyStrings().generateOne,
            "connection-pool" -> positiveInts().generateOne.value
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-user' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map[String, Any](
            "db-host"         -> hosts.generateOne.value,
            "db-user"         -> "",
            "db-port"         -> positiveInts().generateOne.value,
            "db-pass"         -> nonEmptyStrings().generateOne,
            "connection-pool" -> positiveInts().generateOne.value
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.db-pass' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map[String, Any](
            "db-host"         -> hosts.generateOne.value,
            "db-user"         -> nonEmptyStrings().generateOne,
            "db-port"         -> positiveInts().generateOne.value,
            "connection-pool" -> positiveInts().generateOne.value
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try, TestDB](namespace, dbName, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.connection-pool' in the config" in new TestCase {
      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map[String, Any](
            "db-host" -> hosts.generateOne.value,
            "db-port" -> positiveInts().generateOne.value,
            "db-user" -> nonEmptyStrings().generateOne,
            "db-pass" -> nonEmptyStrings().generateOne
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

    val dbName = RefType
      .applyRef[DbName](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid dbName value"))

    val hosts: Gen[Host] = for {
      hostname <- nonEmptyStrings()
    } yield RefType
      .applyRef[Host](s"$hostname")
      .getOrElse(throw new IllegalArgumentException("Invalid host` value"))

    val ports: Gen[Port] = positiveInts().map(p =>
      RefType.applyRef[Port](p.value).getOrElse(throw new IllegalArgumentException("Invalid port` value"))
    )
  }

}

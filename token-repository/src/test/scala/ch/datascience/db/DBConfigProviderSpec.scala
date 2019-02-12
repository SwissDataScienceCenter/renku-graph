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
import ch.datascience.db.DBConfigProvider.DBConfig.{Driver, Url}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import com.typesafe.config.ConfigFactory
import eu.timepit.refined.api.RefType
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class DBConfigProviderSpec extends WordSpec {

  private implicit val context: MonadError[Try, Throwable] = MonadError[Try, Throwable]

  "get" should {

    "return db config read from the configuration" in new TestCase {
      val namespace = nonEmptyStrings().generateOne
      val user      = nonEmptyStrings().generateOne
      val password  = nonEmptyStrings().generateOne

      val config = ConfigFactory.parseMap(
        Map(
          namespace -> Map(
            "db-user" -> user,
            "db-pass" -> password
          ).asJava
        ).asJava
      )

      val Success(dbConfig) = new DBConfigProvider[Try](namespace, driver, url, config).get()

      dbConfig.driver     shouldBe driver
      dbConfig.url        shouldBe url
      dbConfig.user.value shouldBe user
      dbConfig.pass       shouldBe password
    }

    "fail if there is no db config namespace in the config" in new TestCase {
      val Failure(exception) = new DBConfigProvider[Try]("unknown", driver, url, ConfigFactory.empty()).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.user' in the config" in new TestCase {
      val namespace = nonEmptyStrings().generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "projects-tokens" -> Map(
            "db-user" -> "",
            "db-pass" -> nonEmptyStrings().generateOne
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try](namespace, driver, url, config).get()

      exception shouldBe a[ConfigLoadingException]
    }

    "fail if there is no '<config-namespace>.password' in the config" in new TestCase {
      val namespace = nonEmptyStrings().generateOne
      val config = ConfigFactory.parseMap(
        Map(
          "projects-tokens" -> Map(
            "db-user" -> nonEmptyStrings().generateOne
          ).asJava
        ).asJava
      )

      val Failure(exception) = new DBConfigProvider[Try](namespace, driver, url, config).get()

      exception shouldBe a[ConfigLoadingException]
    }
  }

  private trait TestCase {
    val driver = RefType
      .applyRef[Driver](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid driver value"))
    val url = RefType
      .applyRef[Url](nonEmptyStrings().generateOne)
      .getOrElse(throw new IllegalArgumentException("Invalid url value"))
  }
}

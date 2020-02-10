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

package ch.datascience.dbeventlog.config

import cats.implicits._
import ch.datascience.config.ConfigLoader.ConfigLoadingException
import ch.datascience.dbeventlog.DbEventLogGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.durations
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class RenkuLogTimeoutSpec extends WordSpec with ScalaCheckPropertyChecks {

  "apply" should {

    "read 'renku-log-timeout' from the config" in {
      forAll(durations(max = 10 days)) { duration =>
        val config = ConfigFactory.parseMap(
          Map("renku-log-timeout" -> duration.toString()).asJava
        )

        RenkuLogTimeout[Try](config) shouldBe Success(RenkuLogTimeout(duration))
      }
    }

    "fail if there's no 'renku-log-timeout' entry" in {
      val Failure(exception) = RenkuLogTimeout[Try](ConfigFactory.empty())

      exception shouldBe an[ConfigLoadingException]
    }
  }

  "toUnsafe[java.time.Duration]" should {

    "convert the timeout to java.time.Duration" in {
      forAll { timeout: RenkuLogTimeout =>
        timeout.toUnsafe[java.time.Duration] shouldBe java.time.Duration.ofMillis(timeout.value.toMillis)
      }
    }
  }
}

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

package ch.datascience.webhookservice.missedevents

import cats.MonadError
import cats.implicits._
import com.typesafe.config.ConfigFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

class SchedulerConfigProviderSpec extends WordSpec {

  "getInitialDelay" should {

    "return value from events-synchronization.initial-delay" in new TestCase {
      configLoader.getInitialDelay shouldBe context.pure(10 minutes)
    }
  }

  "getInterval" should {
    "return value from events-synchronization.interval" in new TestCase {
      configLoader.getInterval shouldBe context.pure(2 hours)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val config = ConfigFactory.parseMap(
      Map(
        "events-synchronization" -> Map(
          "initial-delay" -> "10 minutes",
          "interval"      -> "2 hours"
        ).asJava
      ).asJava
    )

    val configLoader = new SchedulerConfigProvider[Try](config)
  }
}

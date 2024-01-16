/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.config

import cats.effect.IO
import com.typesafe.config.ConfigFactory
import io.renku.interpreters.TestLogger
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.config.TriplesGeneration._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class TriplesGenerationSpec extends AnyWordSpec with IOSpec with should.Matchers {

  "apply" should {

    "return RenkuLog with the defaults from application.conf" in {
      TriplesGeneration[IO]().unsafeRunSync() shouldBe RenkuLog
    }

    "return RenkuLog if 'triples-generation' config value is set to 'renku-log'" in {
      val config = ConfigFactory.parseMap(
        Map("triples-generation" -> "renku-log").asJava
      )

      TriplesGeneration[IO](config).unsafeRunSync() shouldBe RenkuLog
    }

    "return an instance of RemoteTriplesGeneration if 'triples-generation' config value is set to 'remote-generator'" in {

      val config = ConfigFactory.parseMap(
        Map("triples-generation" -> "remote-generator").asJava
      )

      TriplesGeneration[IO](config).unsafeRunSync() shouldBe RemoteTriplesGeneration
    }
  }

  private implicit val logger: TestLogger[IO] = TestLogger[IO]()
}

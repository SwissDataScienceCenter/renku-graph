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

package io.renku.microservices

import cats.effect.IO
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.http.client.ServiceHealthChecker
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Info
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ServiceReadinessCheckerSpec extends AnyWordSpec with IOSpec with should.Matchers with MockFactory {

  "waitIfNotUp" should {
    "block until the health check returns true" in new TestCase {
      (urlFinder.findBaseUrl _).expects().returning(microserviceUrl.pure[IO])

      (healthChecker.ping _).expects(microserviceUrl).returning(false.pure[IO])
      (healthChecker.ping _).expects(microserviceUrl).returning(true.pure[IO])

      readinessChecker.waitIfNotUp.unsafeRunSync() shouldBe ()

      logger.loggedOnly(Info("Service not ready"))
    }
  }

  private trait TestCase {
    val microserviceUrl = microserviceBaseUrls.generateOne

    implicit val logger: TestLogger[IO] = TestLogger[IO]()
    val urlFinder        = mock[MicroserviceUrlFinder[IO]]
    val healthChecker    = mock[ServiceHealthChecker[IO]]
    val readinessChecker = new ServiceReadinessCheckerImpl[IO](urlFinder, healthChecker)
  }
}

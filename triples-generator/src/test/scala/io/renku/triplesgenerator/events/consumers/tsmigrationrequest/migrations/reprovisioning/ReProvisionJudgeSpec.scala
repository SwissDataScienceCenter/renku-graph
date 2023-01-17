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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.versions.RenkuVersionPair
import io.renku.http.client.ServiceHealthChecker
import io.renku.interpreters.TestLogger
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import io.renku.triplesgenerator.config.VersionCompatibilityConfig
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ReProvisionJudgeSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "reProvisioningNeeded" should {

    "return false when versions don't match and re-provisioning-needed is false" in new TestCase {
      val compatConfig1 = compatibilityGen.generateOne.copy(reProvisioningNeeded = false)
      val compatConfig2 = compatibilityGen.suchThat(_ != compatConfig1).generateOne.asVersionPair

      (versionPairFinder.find _).expects().returning(Try(compatConfig2.some))
      (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])
      judge(compatConfig1).reProvisioningNeeded() shouldBe Try(false)
    }

    "return false when versions match while re-provisioning-needed is set to true" in new TestCase {
      val compatConfig = compatibilityGen.generateOne.copy(reProvisioningNeeded = true)
      (versionPairFinder.find _).expects().returning(Try(compatConfig.asVersionPair.some))
      (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])
      judge(compatConfig).reProvisioningNeeded() shouldBe Try(false)
    }

    "return true when TS schema version cannot be found" in new TestCase {
      (versionPairFinder.find _).expects().returning(Option.empty[RenkuVersionPair].pure[Try])

      judge(compatibilityGen.generateOne).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return true when TS schema version is different than the config schema version and re-provisioning-needed is true" in new TestCase {
      val tsVersionPair = renkuVersionPairs.generateOne
      (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

      val configVersionPairs = compatibilityGen
        .suchThat(_.schemaVersion != tsVersionPair.schemaVersion)
        .map(_.copy(reProvisioningNeeded = true))

      judge(configVersionPairs.generateOne).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return false when TS schema version is the same as the config schema version " +
      "and there's no ongoing re-provisioning" in new TestCase {
        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        val configVersion = VersionCompatibilityConfig(tsVersionPair, true)

        (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])

        judge(configVersion).reProvisioningNeeded() shouldBe false.pure[Try]
      }

    "return false if schema and CLI version checks resolve to false " +
      "and there is ongoing re-provisioning " +
      "and the service running the process has different url that this service url " +
      "and the service is up" in new TestCase {

        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        val controller = microserviceBaseUrls.generateOne
        (reProvisioningStatus.findReProvisioningService _).expects().returning(controller.some.pure[Try])

        (microserviceUrlFinder.findBaseUrl _).expects().returning(microserviceBaseUrls.generateOne.pure[Try])

        (serviceHealthChecker.ping _).expects(controller).returning(true.pure[Try])

        val matrixVersionPairs = VersionCompatibilityConfig(tsVersionPair, false)

        judge(matrixVersionPairs).reProvisioningNeeded() shouldBe false.pure[Try]
      }

    "return true if schema and CLI version checks resolve to false " +
      "but there's ongoing re-provisioning " +
      "and the service running the process has different url that this service url " +
      "and the service is down" in new TestCase {

        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        val controller = microserviceBaseUrls.generateOne
        (reProvisioningStatus.findReProvisioningService _).expects().returning(controller.some.pure[Try])

        (microserviceUrlFinder.findBaseUrl _).expects().returning(microserviceBaseUrls.generateOne.pure[Try])

        (serviceHealthChecker.ping _).expects(controller).returning(false.pure[Try])

        val configVersion = VersionCompatibilityConfig(tsVersionPair, false)

        judge(configVersion).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "return true if schema and CLI version checks resolve to false " +
      "but there's ongoing re-provisioning " +
      "and the service running the process has the same url as this service" in new TestCase {

        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        val controller = microserviceBaseUrls.generateOne
        (reProvisioningStatus.findReProvisioningService _).expects().returning(controller.some.pure[Try])

        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[Try])

        val configVersion = VersionCompatibilityConfig(tsVersionPair, false)

        judge(configVersion).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "return true if schema and CLI version checks resolve to false " +
      "but there's ongoing re-provisioning " +
      "and there's no info about service running the process" in new TestCase {

        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        (reProvisioningStatus.findReProvisioningService _)
          .expects()
          .returning(Option.empty[MicroserviceBaseUrl].pure[Try])

        val configVersion = VersionCompatibilityConfig(tsVersionPair, false)

        judge(configVersion).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "fail if finding the TS version pair fails" in new TestCase {
      val exception = exceptions.generateOne
      (versionPairFinder.find _).expects().returning(exception.raiseError[Try, Option[RenkuVersionPair]])

      judge(compatibilityGen.generateOne).reProvisioningNeeded() shouldBe exception
        .raiseError[Try, Boolean]
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val versionPairFinder     = mock[RenkuVersionPairFinder[Try]]
    val reProvisioningStatus  = mock[ReProvisioningStatus[Try]]
    val microserviceUrlFinder = mock[MicroserviceUrlFinder[Try]]
    val serviceHealthChecker  = mock[ServiceHealthChecker[Try]]
    def judge(compatibility: VersionCompatibilityConfig) = new ReProvisionJudgeImpl(versionPairFinder,
                                                                                    reProvisioningStatus,
                                                                                    microserviceUrlFinder,
                                                                                    serviceHealthChecker,
                                                                                    compatibility
    )
  }
}

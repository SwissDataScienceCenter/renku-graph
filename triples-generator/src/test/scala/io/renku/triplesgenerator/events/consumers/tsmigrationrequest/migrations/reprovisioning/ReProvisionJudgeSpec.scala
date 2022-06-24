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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import cats.data.NonEmptyList
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.cliVersions
import io.renku.graph.model.RenkuVersionPair
import io.renku.http.client.ServiceHealthChecker
import io.renku.interpreters.TestLogger
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ReProvisionJudgeSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "reProvisioningNeeded" should {

    "return true when TS schema version cannot be found" in new TestCase {
      (versionPairFinder.find _).expects().returning(Option.empty[RenkuVersionPair].pure[Try])

      judge(NonEmptyList.one(renkuVersionPairs.generateOne)).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return true when TS schema version is different than the matrix schema version" in new TestCase {
      val tsVersionPair = renkuVersionPairs.generateOne
      (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

      val matrixVersionPairs = renkuVersionPairs.generateNonEmptyList(minElements = 2)

      judge(matrixVersionPairs).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return true when the top two schema versions in the matrix are the same " +
      "but TS CLI version is different than the top CLI version in the matrix" in new TestCase {
        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        val matrixVersionPairs = renkuVersionPairs
          .generateNonEmptyList(minElements = 2)
          .map(_.copy(schemaVersion = tsVersionPair.schemaVersion))

        judge(matrixVersionPairs).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "return false when TS schema version is the same as the matrix schema version " +
      "and the top two schema versions in the matrix are different " +
      "and there's no ongoing re-provisioning" in new TestCase {
        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        val matrixVersionPair = NonEmptyList.of(
          tsVersionPair.copy(cliVersion = cliVersions.generateOne),
          renkuVersionPairs.generateOne
        )

        (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])

        judge(matrixVersionPair).reProvisioningNeeded() shouldBe false.pure[Try]
      }

    "return false when TS schema version is the same as the matrix schema version " +
      "and there's a single row in the matrix" +
      "and there's no ongoing re-provisioning" in new TestCase {
        val tsVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(tsVersionPair.some.pure[Try])

        val matrixVersionPair = renkuVersionPairs.generateOne.copy(schemaVersion = tsVersionPair.schemaVersion)

        (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])

        judge(NonEmptyList.one(matrixVersionPair)).reProvisioningNeeded() shouldBe false.pure[Try]
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

        val matrixVersionPairs = NonEmptyList.of(tsVersionPair)

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

        val matrixVersionPairs = NonEmptyList.of(tsVersionPair)

        judge(matrixVersionPairs).reProvisioningNeeded() shouldBe true.pure[Try]
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

        val matrixVersionPairs = NonEmptyList.of(tsVersionPair)

        judge(matrixVersionPairs).reProvisioningNeeded() shouldBe true.pure[Try]
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

        val matrixVersionPairs = NonEmptyList.of(tsVersionPair)

        judge(matrixVersionPairs).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "fail if finding the TS version pair fails" in new TestCase {
      val exception = exceptions.generateOne
      (versionPairFinder.find _).expects().returning(exception.raiseError[Try, Option[RenkuVersionPair]])

      judge(NonEmptyList.one(renkuVersionPairs.generateOne)).reProvisioningNeeded() shouldBe exception
        .raiseError[Try, Boolean]
    }
  }

  private trait TestCase {
    implicit val logger: TestLogger[Try] = TestLogger[Try]()
    val versionPairFinder     = mock[RenkuVersionPairFinder[Try]]
    val reProvisioningStatus  = mock[ReProvisioningStatus[Try]]
    val microserviceUrlFinder = mock[MicroserviceUrlFinder[Try]]
    val serviceHealthChecker  = mock[ServiceHealthChecker[Try]]
    def judge(compatibilityMatrix: NonEmptyList[RenkuVersionPair]) = new ReProvisionJudgeImpl(versionPairFinder,
                                                                                              reProvisioningStatus,
                                                                                              microserviceUrlFinder,
                                                                                              serviceHealthChecker,
                                                                                              compatibilityMatrix
    )
  }
}

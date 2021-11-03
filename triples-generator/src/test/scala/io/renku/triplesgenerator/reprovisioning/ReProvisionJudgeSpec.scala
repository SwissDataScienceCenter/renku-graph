/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.reprovisioning

import cats.data.NonEmptyList
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators.microserviceBaseUrls
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.cliVersions
import io.renku.graph.model.RenkuVersionPair
import io.renku.http.client.ServiceHealthChecker
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceUrlFinder}
import io.renku.triplesgenerator.generators.VersionGenerators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class ReProvisionJudgeSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "reProvisioningNeeded" should {

    "return true when the matrix schema version is different than the current schema version in KG" in new TestCase {

      /** current cli version is 1.2.1 current schema version 12 [1.2.3 -> 13, 1.2.1 -> 12] OR for a RollBack current
        * cli version is 1.2.1 current schema version 12 [1.2.3 -> 11, 1.2.1 -> 12]
        */

      val currentVersionPair = renkuVersionPairs.generateOne
      (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

      val newVersionPair = renkuVersionPairs.generateNonEmptyList(minElements = 2)

      judge(newVersionPair).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return false when the matrix schema version is the same as the current schema version in KG" in new TestCase {

      /** current cli version is 1.2.4 current schema version 13 [1.2.3 -> 13, 1.2.1 -> 12] */

      val currentVersionPair = renkuVersionPairs.generateOne
      (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

      val newVersionPair = NonEmptyList.of(
        currentVersionPair.copy(cliVersion = cliVersions.generateOne),
        renkuVersionPairs.generateOne
      )

      (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])

      judge(newVersionPair).reProvisioningNeeded() shouldBe false.pure[Try]
    }

    "return true when the last two schema versions are the same " +
      "but the top cli version in the matrix is different than the current cli version in KG" in new TestCase {

        /** current cli version is 1.2.1 current schema version 13 [1.2.3 -> 13, 1.2.1 -> 13] */

        val currentVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

        val newVersionPair = renkuVersionPairs
          .generateNonEmptyList(minElements = 2)
          .map(_.copy(schemaVersion = currentVersionPair.schemaVersion))

        judge(newVersionPair).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "return false when the schema versions are the same even when the cli versions are different " +
      "- case of a single row in the compatibility matrix" in new TestCase {

        /** current cli version is 1.2.1 current schema version 13 Compat matrix: [1.2.3 -> 13] */

        val currentVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

        val newVersionPair = renkuVersionPairs.generateOne.copy(schemaVersion = currentVersionPair.schemaVersion)

        (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])

        judge(NonEmptyList.one(newVersionPair)).reProvisioningNeeded() shouldBe false.pure[Try]
      }

    "return true when the current schema version is None" in new TestCase {
      (versionPairFinder.find _).expects().returning(Option.empty[RenkuVersionPair].pure[Try])

      judge(NonEmptyList.one(renkuVersionPairs.generateOne)).reProvisioningNeeded() shouldBe true.pure[Try]
    }

    "return false if the matrix schema version is the same as the current schema version in KG " +
      "and the re-provisioning status in KG is 'running' " +
      "and the service running the process has different url that this service " +
      "and it's up" in new TestCase {

        val currentVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        val controller = microserviceBaseUrls.generateOne
        (reProvisioningStatus.findReProvisioningService _).expects().returning(controller.some.pure[Try])

        (microserviceUrlFinder.findBaseUrl _).expects().returning(microserviceBaseUrls.generateOne.pure[Try])

        (serviceHealthChecker.ping _).expects(controller).returning(true.pure[Try])

        val newVersionPair = NonEmptyList.of(
          currentVersionPair.copy(cliVersion = cliVersions.generateOne),
          renkuVersionPairs.generateOne
        )

        judge(newVersionPair).reProvisioningNeeded() shouldBe false.pure[Try]
      }

    "return true even if the matrix schema version is the same as the current schema version in KG " +
      "but the re-provisioning status in KG is 'running' " +
      "and the service running the process is not this service and is accessible" in new TestCase {

        val currentVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        val controller = microserviceBaseUrls.generateOne
        (reProvisioningStatus.findReProvisioningService _).expects().returning(controller.some.pure[Try])

        (microserviceUrlFinder.findBaseUrl _).expects().returning(microserviceBaseUrls.generateOne.pure[Try])

        (serviceHealthChecker.ping _).expects(controller).returning(false.pure[Try])

        val newVersionPair = NonEmptyList.of(
          currentVersionPair.copy(cliVersion = cliVersions.generateOne),
          renkuVersionPairs.generateOne
        )

        judge(newVersionPair).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "return true even if the matrix schema version is the same as the current schema version in KG " +
      "but the re-provisioning status in KG is 'running' " +
      "and the service running the process has the same url as this service" in new TestCase {

        val currentVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        val controller = microserviceBaseUrls.generateOne
        (reProvisioningStatus.findReProvisioningService _).expects().returning(controller.some.pure[Try])

        (microserviceUrlFinder.findBaseUrl _).expects().returning(controller.pure[Try])

        val newVersionPair = NonEmptyList.of(
          currentVersionPair.copy(cliVersion = cliVersions.generateOne),
          renkuVersionPairs.generateOne
        )

        judge(newVersionPair).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "return true even if the matrix schema version is the same as the current schema version in KG " +
      "but the re-provisioning status in KG is 'running' " +
      "and there's no info about service running the process" in new TestCase {

        val currentVersionPair = renkuVersionPairs.generateOne
        (versionPairFinder.find _).expects().returning(currentVersionPair.some.pure[Try])

        (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

        (reProvisioningStatus.findReProvisioningService _)
          .expects()
          .returning(Option.empty[MicroserviceBaseUrl].pure[Try])

        val newVersionPair = NonEmptyList.of(
          currentVersionPair.copy(cliVersion = cliVersions.generateOne),
          renkuVersionPairs.generateOne
        )

        judge(newVersionPair).reProvisioningNeeded() shouldBe true.pure[Try]
      }

    "fail if finding the current version pair in KG fails" in new TestCase {
      val exception = exceptions.generateOne
      (versionPairFinder.find _).expects().returning(exception.raiseError[Try, Option[RenkuVersionPair]])

      judge(NonEmptyList.one(renkuVersionPairs.generateOne)).reProvisioningNeeded() shouldBe exception
        .raiseError[Try, Boolean]
    }
  }

  private trait TestCase {
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

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

package io.renku.triplesgenerator.events.consumers

import TSStateChecker.TSState
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.triplesstore.{DatasetName, TSAdminClient}
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TSStateCheckerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "checkTSState" should {

    "return Ready when all datasets are created and no re-provisioning is running" in new TestCase {

      datasets foreach {
        (rdfStoreAdmin.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])

      stateChecker.checkTSState shouldBe TSState.Ready.pure[Try]
    }

    "return ReProvisioning when all datasets are created but re-provisioning is running" in new TestCase {

      datasets foreach {
        (rdfStoreAdmin.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

      stateChecker.checkTSState shouldBe TSState.ReProvisioning.pure[Try]
    }

    "return MissingDatasets when one or more datasets are not created" in new TestCase {

      (rdfStoreAdmin.checkDatasetExists _).expects(datasets.head).returning(false.pure[Try])
      datasets.tail foreach {
        (rdfStoreAdmin.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      stateChecker.checkTSState shouldBe TSState.MissingDatasets.pure[Try]
    }

    "fail if at least one datasets existence check fails" in new TestCase {
      val exception = exceptions.generateOne
      (rdfStoreAdmin.checkDatasetExists _).expects(datasets.head).returning(exception.raiseError[Try, Boolean])
      datasets.tail foreach {
        (rdfStoreAdmin.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      stateChecker.checkTSState shouldBe exception.raiseError[Try, Boolean]
    }

    "fail if re-provisioning check fails" in new TestCase {

      datasets foreach {
        (rdfStoreAdmin.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      val exception = exceptions.generateOne
      (reProvisioningStatus.underReProvisioning _).expects().returning(exception.raiseError[Try, Boolean])

      stateChecker.checkTSState shouldBe exception.raiseError[Try, Boolean]
    }
  }

  private trait TestCase {
    val datasets             = nonEmptyStrings().toGeneratorOf(DatasetName).generateNonEmptyList().toList
    val rdfStoreAdmin        = mock[TSAdminClient[Try]]
    val reProvisioningStatus = mock[ReProvisioningStatus[Try]]
    val stateChecker         = new TSStateCheckerImpl[Try](datasets, rdfStoreAdmin, reProvisioningStatus)
  }
}

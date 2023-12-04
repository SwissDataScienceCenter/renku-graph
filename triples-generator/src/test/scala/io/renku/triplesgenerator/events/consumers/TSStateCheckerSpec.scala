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

package io.renku.triplesgenerator.events.consumers

import TSStateChecker.TSState
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, nonEmptyStrings}
import io.renku.triplesstore.{DatasetName, TSAdminClient}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tsmigrationrequest.MigrationStatusChecker
import tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus

import scala.util.Try

class TSStateCheckerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "checkTSState" should {

    "return Ready when all datasets are created and " +
      "no re-provisioning or migration is running" in new TestCase {

        datasets foreach { (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try]) }
        (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])
        (() => migrationStatusChecker.underMigration).expects().returning(false.pure[Try])

        stateChecker.checkTSState shouldBe TSState.Ready.pure[Try]
      }

    "return Migrating when all datasets are created, no ongoing re-provision but some migration is running" in new TestCase {

      datasets foreach { (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try]) }
      (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])
      (() => migrationStatusChecker.underMigration).expects().returning(true.pure[Try])

      stateChecker.checkTSState shouldBe TSState.Migrating.pure[Try]
    }

    "return ReProvisioning when all datasets are created but re-provisioning is running" in new TestCase {

      datasets foreach {
        (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

      stateChecker.checkTSState shouldBe TSState.ReProvisioning.pure[Try]
    }

    "return MissingDatasets when one or more datasets are not created" in new TestCase {

      (tsAdminClient.checkDatasetExists _).expects(datasets.head).returning(false.pure[Try])
      datasets.tail foreach {
        (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      stateChecker.checkTSState shouldBe TSState.MissingDatasets.pure[Try]
    }

    "fail if at least one datasets existence check fails" in new TestCase {
      val exception = exceptions.generateOne
      (tsAdminClient.checkDatasetExists _).expects(datasets.head).returning(exception.raiseError[Try, Boolean])
      datasets.tail foreach {
        (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      stateChecker.checkTSState shouldBe exception.raiseError[Try, Boolean]
    }

    "fail if re-provisioning check fails" in new TestCase {

      datasets foreach {
        (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      val exception = exceptions.generateOne
      (reProvisioningStatus.underReProvisioning _).expects().returning(exception.raiseError[Try, Boolean])

      stateChecker.checkTSState shouldBe exception.raiseError[Try, Boolean]
    }
  }

  "checkTSReady" should {

    "return true when all datasets are created and " +
      "no re-provisioning or migration is running" in new TestCase {

        datasets foreach { (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try]) }
        (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])
        (() => migrationStatusChecker.underMigration).expects().returning(false.pure[Try])

        stateChecker.checkTSReady shouldBe true.pure[Try]
      }

    "return false when all datasets are created, no ongoing re-provision but some migration is running" in new TestCase {

      datasets foreach { (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try]) }
      (reProvisioningStatus.underReProvisioning _).expects().returning(false.pure[Try])
      (() => migrationStatusChecker.underMigration).expects().returning(true.pure[Try])

      stateChecker.checkTSReady shouldBe false.pure[Try]
    }

    "return false when all datasets are created but re-provisioning is running" in new TestCase {

      datasets foreach { (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try]) }
      (reProvisioningStatus.underReProvisioning _).expects().returning(true.pure[Try])

      stateChecker.checkTSReady shouldBe false.pure[Try]
    }

    "return false when one or more datasets are not created" in new TestCase {

      (tsAdminClient.checkDatasetExists _).expects(datasets.head).returning(false.pure[Try])
      datasets.tail foreach {
        (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      stateChecker.checkTSReady shouldBe false.pure[Try]
    }

    "fail if at least one datasets existence check fails" in new TestCase {
      val exception = exceptions.generateOne
      (tsAdminClient.checkDatasetExists _).expects(datasets.head).returning(exception.raiseError[Try, Boolean])
      datasets.tail foreach {
        (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      stateChecker.checkTSReady shouldBe exception.raiseError[Try, Boolean]
    }

    "fail if re-provisioning check fails" in new TestCase {

      datasets foreach {
        (tsAdminClient.checkDatasetExists _).expects(_).returning(true.pure[Try])
      }

      val exception = exceptions.generateOne
      (reProvisioningStatus.underReProvisioning _).expects().returning(exception.raiseError[Try, Boolean])

      stateChecker.checkTSReady shouldBe exception.raiseError[Try, Boolean]
    }
  }

  private trait TestCase {
    val datasets               = nonEmptyStrings().toGeneratorOf(DatasetName).generateNonEmptyList().toList
    val tsAdminClient          = mock[TSAdminClient[Try]]
    val reProvisioningStatus   = mock[ReProvisioningStatus[Try]]
    val migrationStatusChecker = mock[MigrationStatusChecker[Try]]
    val stateChecker =
      new TSStateCheckerImpl[Try](datasets, tsAdminClient, reProvisioningStatus, migrationStatusChecker)
  }
}

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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import com.typesafe.config.ConfigFactory
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.metrics.MetricsRegistry.DisabledMetricsRegistry
import io.renku.triplesstore.SparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning.ReProvisioningStatus
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MigrationsSpec extends AnyWordSpec with should.Matchers with IOSpec with MockFactory {

  "apply" should {

    "not raise an error if there are migrations with unique names" in {
      val migrations = Migrations[IO](ConfigFactory.load()).unsafeRunSync()
      migrations.isEmpty shouldBe false
    }
  }

  private implicit lazy val reProvisioningStatus: ReProvisioningStatus[IO]    = mock[ReProvisioningStatus[IO]]
  private implicit lazy val logger:               TestLogger[IO]              = TestLogger[IO]()
  private implicit lazy val timeRecorder:         SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
  private implicit lazy val metricsRegistry:      MetricsRegistry[IO]         = new DisabledMetricsRegistry[IO]
}

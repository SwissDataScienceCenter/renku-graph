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

package ch.datascience.triplesgenerator.reprovisioning

import cats.MonadError
import cats.implicits._
import ch.datascience.dbeventlog.EventStatus.New
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class ReProvisionerSpec extends WordSpec with MockFactory {

  "startReProvisioning" should {

    s"succeed if truncating the RDF store and marking events in the Log with $New succeeds" in new TestCase {

      (datasetTruncator.truncateDataset _)
        .expects()
        .returning(context.unit)

      (eventLogMarkAllNew.markAllEventsAsNew _)
        .expects()
        .returning(context.unit)

      reProvisioner.startReProvisioning shouldBe context.unit
    }

    "fail if truncating the RDF store fails" in new TestCase {

      val exception = exceptions.generateOne
      (datasetTruncator.truncateDataset _)
        .expects()
        .returning(context.raiseError(exception))

      reProvisioner.startReProvisioning shouldBe context.raiseError(exception)
    }

    "fail if marking events in the Log fails" in new TestCase {

      (datasetTruncator.truncateDataset _)
        .expects()
        .returning(context.unit)

      val exception = exceptions.generateOne
      (eventLogMarkAllNew.markAllEventsAsNew _)
        .expects()
        .returning(context.raiseError(exception))

      reProvisioner.startReProvisioning shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val datasetTruncator   = mock[DatasetTruncator[Try]]
    val eventLogMarkAllNew = mock[TryEventLogMarkAllNew]
    val reProvisioner      = new ReProvisioner[Try](datasetTruncator, eventLogMarkAllNew)
  }
}

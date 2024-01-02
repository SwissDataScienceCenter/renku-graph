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

package io.renku.graph.model.entities

import io.renku.cli.model.CliPublicationEvent
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.graph.model.tools.AdditionalMatchers
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class PublicationEventSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with DiffInstances
    with AdditionalMatchers {

  "fromCli" should {

    "turn CLliPublicationEvent entity into the PublicationEvent object" in {
      val startDate = timestampsNotInTheFuture.generateOne
      val dataset   = datasetEntities(provenanceNonModified).decoupledFromProject.generateOne

      forAll(publicationEventFactories(startDate)) { eventFactory: (Dataset[Dataset.Provenance] => PublicationEvent) =>
        val event = eventFactory(dataset)

        val cliEvent = event.to[CliPublicationEvent]
        entities.PublicationEvent.fromCli(cliEvent) shouldMatchToValid event.to[entities.PublicationEvent]
      }
    }
  }
}

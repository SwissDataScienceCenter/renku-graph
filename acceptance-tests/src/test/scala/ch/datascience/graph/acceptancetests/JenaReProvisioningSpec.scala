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

package ch.datascience.graph.acceptancetests

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.acceptancetests.stubs.RemoteTriplesGenerator._
import ch.datascience.graph.acceptancetests.testing.AcceptanceTestPatience
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, RDFStore}
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.EventsGenerators.{commitIds, projects}
import flows.RdfStoreProvisioning._
import model._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class JenaReProvisioningSpec extends FeatureSpec with GivenWhenThen with GraphServices with AcceptanceTestPatience {

  feature("RDF Store re-provisioning with the data from the Event Log") {

    scenario("Re-provisioning the RDF triples with outdated version") {

      GraphServices.restart(triplesGenerator)

      val project  = projects.generateOne
      val commitId = commitIds.generateOne

      Given("the re-provisioning process starts with configured re-provisioning-initial-delay = 5 seconds")
      And("there are outdated triples in the RDF Store")
      val oldSchemaVersion = SchemaVersion("0.4.0")
      `data in the RDF store`(project, commitId, oldSchemaVersion)
      eventually {
        RDFStore.doesVersionTripleExist(oldSchemaVersion) shouldBe true
      }

      When("re-provisioning process detects the outdated triples")
      Then("all the outdated triples get re-provisioned")
      `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project,
                                                                                        commitId,
                                                                                        currentSchemaVersion)
      eventually {
        RDFStore.doesVersionTripleExist(oldSchemaVersion) shouldBe false
      }
      eventually {
        RDFStore.doesVersionTripleExist(currentSchemaVersion) shouldBe true
      }
    }
  }
}

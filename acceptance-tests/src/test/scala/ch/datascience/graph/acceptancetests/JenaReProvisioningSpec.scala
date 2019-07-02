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
import ch.datascience.graph.acceptancetests.tooling.{GraphServices, RDFStore}
import ch.datascience.graph.model.events.EventsGenerators.{commitIds, projects}
import ch.datascience.http.client.{BasicAuthCredentials, BasicAuthPassword, BasicAuthUsername}
import flows.RdfStoreProvisioning._
import org.http4s.Status._
import org.scalatest.Matchers._
import org.scalatest.{FeatureSpec, GivenWhenThen}

class JenaReProvisioningSpec extends FeatureSpec with GivenWhenThen with GraphServices {

  feature("RDF Store re-provisioning with the data from the Event Log") {

    scenario("Complete re-provisioning the RDF Store") {

      val project  = projects.generateOne
      val commitId = commitIds.generateOne

      Given("some data in the RDF Store")
      `data in the RDF store`(project, commitId)
      val initialTriplesNumber = RDFStore.findAllTriplesNumber()

      When("user does DELETE triples-generator/triples/projects")
      triplesGeneratorClient
        .DELETE("triples/projects", BasicAuthCredentials(BasicAuthUsername("admin"), BasicAuthPassword("adminpass")))
        .status shouldBe Accepted

      Then("all the triples should be deleted")
      eventually {
        RDFStore.findAllTriplesNumber() shouldBe 0
      }

      And("then all the RDF Store gets re-provisioned with the events from the Log")
      eventually {
        RDFStore.findAllTriplesNumber() shouldBe initialTriplesNumber
      }
    }

    scenario("Re-provisioning endpoint not available with invalid credentials") {

      When("user does DELETE triples-generator/triples/projects with invalid credentials")
      val response = triplesGeneratorClient
        .DELETE("triples/projects", BasicAuthCredentials(BasicAuthUsername("regular"), BasicAuthPassword("user")))

      Then("he gets FORBIDDEN status")
      response.status shouldBe Unauthorized
    }
  }
}

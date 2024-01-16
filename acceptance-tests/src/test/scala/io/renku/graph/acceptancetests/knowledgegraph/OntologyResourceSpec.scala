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

package io.renku.graph.acceptancetests.knowledgegraph

import io.renku.graph.acceptancetests.tooling.{AcceptanceSpec, ApplicationServices}
import org.http4s.MediaType.text
import org.http4s.Status.Ok
import org.http4s.headers.`Content-Type`

class OntologyResourceSpec extends AcceptanceSpec with ApplicationServices {

  Feature("GET knowledge-graph/ontology to return Renku ontology specification") {

    Scenario("User navigates to GET knowledge-graph/ontology") {

      Given("User navigates to GET knowledge-graph/ontology")
      val (status, headers, pageBody) = knowledgeGraphClient `GET /knowledge-graph/ontology`

      Then("he should get OK response with ontology HTML page")
      status                      shouldBe Ok
      headers.get[`Content-Type`] shouldBe Some(`Content-Type`(text.html))
      pageBody                      should include("Renku Graph Ontology")
    }
  }
}

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

package io.renku.graph.acceptancetests
package knowledgegraph

import io.circe.Json
import org.http4s.MediaType.application
import org.http4s.Status.Ok
import org.http4s.headers.`Content-Type`
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should
import tooling.GraphServices

class ApiDocsResourceSpec extends AnyFeatureSpec with GivenWhenThen with GraphServices with should.Matchers {

  Feature("GET knowledge-graph/spec.json to return KG API specs") {

    Scenario("User navigates to GET knowledge-graph/spec.json") {

      Given("User navigates to GET knowledge-graph/spec.json")

      val response = knowledgeGraphClient GET s"knowledge-graph/spec.json"

      Then("he should get OK response with the API specs in JSON")
      response.status                      shouldBe Ok
      response.headers.get[`Content-Type`] shouldBe Some(`Content-Type`(application.json))
      response.jsonBody                      should (not be Json.arr() and not be Json.Null)
    }
  }
}

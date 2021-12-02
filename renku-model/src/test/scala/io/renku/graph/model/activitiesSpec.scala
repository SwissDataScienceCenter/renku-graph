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

package io.renku.graph.model

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class activitiesSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers with Schemas {

  "ResourceId" should {
    "be renderable as RDF resource" in {
      val id = httpUrls().generateAs[activities.ResourceId]
      id.showAs[RdfResource] shouldBe s"<${sparqlEncode(id.value)}>"
    }
  }

}

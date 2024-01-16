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

package io.renku.knowledgegraph.ontology

import cats.data.NonEmptyList
import io.renku.entities.searchgraphs.datasets.DatasetSearchInfoOntology
import io.renku.graph.model.Schemas
import io.renku.graph.model.entities.{CompositePlan, Project}
import io.renku.jsonld.ontology._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class OntologyGeneratorSpec extends AnyWordSpec with should.Matchers {

  "getOntology" should {

    "return generated Renku ontology" in {
      val types =
        NonEmptyList.of(Project.Ontology.typeDef, CompositePlan.Ontology.typeDef, DatasetSearchInfoOntology.typeDef)
      val ontology = generateOntology(types, Schemas.renku)

      new OntologyGeneratorImpl(ontology).getOntology shouldBe ontology
    }
  }
}

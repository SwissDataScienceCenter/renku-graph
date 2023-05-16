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

package io.renku.entities.searchgraphs.datasets.commands

import io.renku.entities.searchgraphs.datasets.commands.DatasetsQuad
import io.renku.generators.Generators.Implicits._
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.graph.model.GraphClass
import io.renku.jsonld.{EntityId, EntityIdEncoder}
import io.renku.triplesstore.client.TriplesStoreGenerators.tripleObjects
import io.renku.triplesstore.client.model.Quad
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class DatasetsQuadSpec extends AnyWordSpec with should.Matchers {

  "apply" should {

    "instantiate a Quad for the Datasets graph" in {
      val subject   = entityIds.generateOne
      val predicate = properties.generateOne
      val obj       = tripleObjects.generateOne

      DatasetsQuad(subject, predicate, obj) shouldBe Quad(GraphClass.Datasets.id, subject, predicate, obj)
    }
  }

  private implicit val entityIdEnc: EntityIdEncoder[EntityId] = EntityIdEncoder.instance(identity)
}

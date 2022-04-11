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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest
package migrations

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.projects
import io.renku.graph.model.testentities._
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class TopMostDerivedFromSpec extends AnyWordSpec with should.Matchers with IOSpec with InMemoryRdfStore {

  "query" should {

    "find projects having at least two modified Datasets" in {
      val (_ ::~ _ ::~ _ ::~ _, project1) =
        anyRenkuProjectEntities
          .addDatasetAndModification(datasetEntities(provenanceNonModified))
          .addDatasetAndModification(datasetEntities(provenanceNonModified))
          .generateOne
      val (_ ::~ _ ::~ _ ::~ _, project2) =
        anyRenkuProjectEntities
          .addDatasetAndModification(datasetEntities(provenanceNonModified))
          .addDatasetAndModification(datasetEntities(provenanceNonModified))
          .generateOne

      loadToStore(
        project1,
        project2,
        anyRenkuProjectEntities.addDatasetAndModification(datasetEntities(provenanceNonModified)).generateOne._2,
        anyRenkuProjectEntities.addDataset(datasetEntities(provenanceNonModified)).generateOne._2,
        anyRenkuProjectEntities.generateOne
      )

      runQuery(TopMostDerivedFrom.query.toString)
        .unsafeRunSync()
        .map(row => projects.Path(row("path")))
        .toSet shouldBe Set(project1.path, project2.path)
    }
  }
}

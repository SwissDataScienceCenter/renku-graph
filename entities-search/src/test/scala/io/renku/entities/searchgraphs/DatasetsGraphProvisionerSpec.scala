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

package io.renku.entities.searchgraphs

import DatasetsCollector._
import SearchInfoExtractor._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class DatasetsGraphProvisionerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "provisionDatasetsGraph" should {

    "collect all the Datasets that are the latest modifications and not invalidations, " +
      "extract the Datasets graph relevant data and " +
      "push the data into the TS" in new TestCase {

        val project = anyRenkuProjectEntities
          .withDatasets(
            List.fill(positiveInts(max = 5).generateOne.value)(datasetEntities(provenanceNonModified)): _*
          )
          .generateOne
          .to[entities.Project]

        givenUploadingDSFrom(project, returning = ().pure[Try])

        provisioner.provisionDatasetsGraph(project) shouldBe ().pure[Try]
      }
  }

  private trait TestCase {
    private val searchInfoUploader = mock[SearchInfoUploader[Try]]
    val provisioner                = new DatasetsGraphProvisionerImpl[Try](searchInfoUploader)

    def givenUploadingDSFrom(project: entities.Project, returning: Try[Unit]) =
      (searchInfoUploader.upload _)
        .expects((collectLastVersions >>> extractSearchInfo(project))(project))
        .returning(returning)
  }
}

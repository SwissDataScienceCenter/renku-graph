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

package io.renku.entities.searchgraphs.datasets

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import commands.Encoders._
import io.renku.entities.EntitiesSearchJenaSpec
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.searchgraphs.datasets.Generators._
import io.renku.entities.searchgraphs.datasets.SearchInfoLens._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class TSSearchInfoFetcherSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with EntitiesSearchJenaSpec
    with TestSearchInfoDatasets
    with OptionValues
    with should.Matchers {

  "findTSInfosByProject" should {

    "find info about all Datasets that are linked to the Project" in projectsDSConfig.use { implicit pcc =>
      val project      = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      val otherProject = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      val infos = datasetSearchInfoObjects(project)
        .generateList(min = 1)
        .map { si =>
          val linkToOtherProject =
            updateLinkProject(otherProject)(linkObjectsGen(si.topmostSameAs).generateOne)

          searchInfoLinks.modify(_ append linkToOtherProject)(si)
        }

      insert(infos.flatMap(_.asQuads)) >>
        insert(datasetSearchInfoObjects.generateOne.asQuads.toList) >>
        List(project, otherProject).traverse_(insertProjectAuth) >>
        fetcher
          .findTSInfosByProject(project.resourceId)
          .asserting(_ should contain theSameElementsAs infos.map(toTSSearchInfo).map(orderValues))
    }

    "return nothing if no Datasets for the Project" in projectsDSConfig.use { implicit pcc =>
      insert(datasetSearchInfoObjects.generateOne.asQuads.toList) >>
        fetcher.findTSInfosByProject(projectResourceIds.generateOne).asserting(_ shouldBe Nil)
    }
  }

  "findTSInfoBySameAs" should {

    "find an info if exists in the TS" in projectsDSConfig.use { implicit pcc =>
      val project      = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      val otherProject = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      val info = datasetSearchInfoObjects(project).map { si =>
        val linkToOtherProject =
          updateLinkProject(otherProject)(linkObjectsGen(si.topmostSameAs).generateOne)
        searchInfoLinks.modify(_ append linkToOtherProject)(si)
      }.generateOne

      insert(info.asQuads.toList) >>
        insert(datasetSearchInfoObjects.generateOne.asQuads.toList) >>
        List(project, otherProject).traverse_(insertProjectAuth) >>
        fetcher
          .findTSInfoBySameAs(info.topmostSameAs)
          .asserting(_.value shouldBe orderValues(toTSSearchInfo(info)))
    }

    "return None if there's no DS with the given topmostSameAs" in projectsDSConfig.use { implicit pcc =>
      fetcher
        .findTSInfoBySameAs(datasetTopmostSameAs.generateOne)
        .asserting(_ shouldBe None)
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private def fetcher(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new TSSearchInfoFetcherImpl[IO](pcc)
  }

  private def toTSSearchInfo(info: DatasetSearchInfo): TSDatasetSearchInfo =
    TSDatasetSearchInfo(info.topmostSameAs, info.links.toList)

  private def orderValues(info: TSDatasetSearchInfo) =
    info.copy(links = info.links.sortBy(_.projectId))
}

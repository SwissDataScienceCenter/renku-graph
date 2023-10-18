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

package io.renku.entities.searchgraphs.datasets
package commands

import Encoders._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.SearchInfoDatasets
import io.renku.entities.searchgraphs.datasets.Generators._
import io.renku.entities.searchgraphs.datasets.SearchInfoLens._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class TSSearchInfoFetcherSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with SearchInfoDatasets {

  it should "find info about all Datasets that are linked to the Project" in {

    val project      = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
    val otherProject = anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
    val infos = datasetSearchInfoObjects(project)
      .generateList(min = 1)
      .map { si =>
        val linkToOtherProject =
          updateLinkProject(otherProject)(linkObjectsGen(si.topmostSameAs).generateOne)

        searchInfoLinks.modify(_ append linkToOtherProject)(si)
      }

    insert(projectsDataset, infos.map(_.asQuads).toSet.flatten)

    // other project DS
    insert(projectsDataset, datasetSearchInfoObjects.generateOne.asQuads)

    List(project, otherProject).traverse_(insertProjectAuth) >>
      fetcher
        .fetchTSSearchInfos(project.resourceId)
        .asserting(_ should contain theSameElementsAs infos.map(toTSSearchInfo).map(orderValues))
  }

  it should "return nothing if no Datasets for the Project" in {

    insert(projectsDataset, datasetSearchInfoObjects.generateOne.asQuads)

    fetcher.fetchTSSearchInfos(projectResourceIds.generateOne).asserting(_ shouldBe Nil)
  }

  implicit val ioLogger:             TestLogger[IO]              = TestLogger[IO]()
  private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
  private lazy val fetcher = new TSSearchInfoFetcherImpl[IO](projectsDSConnectionInfo)

  private def toTSSearchInfo(info: DatasetSearchInfo): TSDatasetSearchInfo =
    TSDatasetSearchInfo(info.topmostSameAs, info.links.toList)

  private def orderValues(info: TSDatasetSearchInfo) =
    info.copy(links = info.links.sortBy(_.projectId))
}

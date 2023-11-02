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
import io.renku.entities.searchgraphs.UpdateCommand._
import io.renku.entities.searchgraphs.datasets.Generators._
import io.renku.entities.searchgraphs.datasets.{DatasetSearchInfoOntology, Link}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.datasets.TopmostSameAs
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class CommandsCalculatorSpec extends AnyFlatSpec with should.Matchers with OptionValues {

  private val calculateCommands = CommandsCalculator.calculateCommands

  it should "create Inserts for the calculated info " +
    "when only model info exists" in {

      val project = createProject

      val modelInfo = modelDatasetSearchInfoObjects(withLinkTo = project).generateOne

      val infoSet = CalculatorInfoSet.ModelInfoOnly(project.identification, modelInfo)

      calculateCommands(infoSet) shouldBe infoSet.asDatasetSearchInfo.value.asQuads.map(Insert).toList
    }

  it should "return the Info delete query " +
    "when only TS info exists and it has link to the only project" in {

      val project = createProject

      val tsInfo = tsDatasetSearchInfoObjects(withLinkTo = project).generateOne

      val infoSet = CalculatorInfoSet.TSInfoOnly(project.identification, tsInfo)

      calculateCommands(infoSet) shouldBe List(Query(DatasetInfoDeleteQuery(tsInfo.topmostSameAs)))
    }

  it should "return a link delete query, a projectsVisibilitiesConcat triple delete command and projectsVisibilitiesConcat triple insert command " +
    "when only TS info exists and it has links to multiple projects" in {

      val project = createProject

      val tsInfo = tsDatasetSearchInfoObjects(withLinkTo = project.resourceId,
                                              projectResourceIds.generateList(min = 1): _*
      ).generateOne

      val infoSet = CalculatorInfoSet.TSInfoOnly(project.identification, tsInfo)

      calculateCommands(infoSet) shouldBe List(
        Query(
          DatasetLinkDeleteQuery(tsInfo.topmostSameAs,
                                 tsInfo.links.find(_.projectId == project.resourceId).value.resourceId
          )
        ),
        Query(SlugVisibilitiesConcatDeleteQuery(tsInfo.topmostSameAs)),
        Insert(projectsVisibilitiesConcatQuad(tsInfo.topmostSameAs, tsInfo.links))
      )
    }

  it should "return the Info delete query and Inserts for the calculated info " +
    "when there are both model and TS infos" in {

      val project = createProject

      val modelInfo = modelDatasetSearchInfoObjects(withLinkTo = project).generateOne
      val tsInfo    = tsDatasetSearchInfoObjects(withLinkTo = project, modelInfo.topmostSameAs).generateOne

      val infoSet = CalculatorInfoSet.BothInfos(project.identification, modelInfo, tsInfo)

      calculateCommands(infoSet) shouldBe
        Query(DatasetInfoDeleteQuery(tsInfo.topmostSameAs)) ::
        infoSet.asDatasetSearchInfo.value.asQuads.map(Insert).toList
    }

  private def projectsVisibilitiesConcatQuad(topSameAs: TopmostSameAs, links: List[Link]): Quad =
    (topSameAs -> links).asQuads match {
      case qds if qds.size == 1 => qds.head
      case qds =>
        fail(s"${qds.size} quads for ${DatasetSearchInfoOntology.projectsVisibilitiesConcatProperty} instead of one")
    }

  private def createProject: entities.Project =
    anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
}

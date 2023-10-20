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

import CalculatorInfoSetGenerators._
import Generators._
import cats.data.NonEmptyList
import cats.syntax.all._
import io.renku.entities.searchgraphs.datasets.ModelDatasetSearchInfo
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, OptionValues}
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CalculatorInfoSetSpec
    extends AnyWordSpec
    with should.Matchers
    with ScalaCheckPropertyChecks
    with EitherValues
    with OptionValues {

  "from" should {

    "create CalculatorInfoSet when there's no TS Search Info" in {

      val project   = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val modelInfo = modelDatasetSearchInfoObjects(project).generateOne

      val infoSet = CalculatorInfoSet.from(project.identification, modelInfo.some, maybeTSInfo = None).value

      infoSet shouldBe CalculatorInfoSet.ModelInfoOnly(project.identification, modelInfo)
    }

    "create CalculatorInfoSet when there's no model Search Info" in {

      val project = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val tsInfo  = tsDatasetSearchInfoObjects(project.resourceId).generateOne

      val infoSet = CalculatorInfoSet.from(project.identification, maybeModelInfo = None, tsInfo.some).value

      infoSet shouldBe CalculatorInfoSet.TSInfoOnly(project.identification, tsInfo)
    }

    "create CalculatorInfoSet when there are both Search Infos" in {

      val project   = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val modelInfo = modelDatasetSearchInfoObjects(project).generateOne
      val tsInfo =
        tsDatasetSearchInfoObjects(project.resourceId).generateOne.copy(topmostSameAs = modelInfo.topmostSameAs)

      val infoSet = CalculatorInfoSet.from(project.identification, modelInfo.some, tsInfo.some).value

      infoSet shouldBe CalculatorInfoSet.BothInfos(project.identification, modelInfo, tsInfo)
    }

    "fail when there's a model Search Info pointing to another Project" in {

      val project = projectIdentifications.generateOne
      val modelInfo = modelDatasetSearchInfoObjects(
        anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      ).generateOne

      val exception = CalculatorInfoSet.from(project, modelInfo.some, maybeTSInfo = None).left.value

      exception.getMessage shouldBe show"CalculatorInfoSet for ${project.resourceId} has link to ${modelInfo.link.projectId}"
    }

    "fail when model and TS Search Infos point to another TopmostSameAs" in {

      val project   = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val modelInfo = modelDatasetSearchInfoObjects(project).generateOne
      val tsInfo    = tsDatasetSearchInfoObjects(project.resourceId).generateOne

      val exception =
        CalculatorInfoSet.from(project.identification, modelInfo.some, maybeTSInfo = tsInfo.some).left.value

      exception.getMessage shouldBe show"CalculatorInfoSet for ${project.resourceId} has different TopmostSameAs"
    }

    "fail when there's neither model nor TS info" in {

      val project = projectIdentifications.generateOne

      val exception = CalculatorInfoSet.from(project, maybeModelInfo = None, maybeTSInfo = None).left.value

      exception.getMessage shouldBe show"CalculatorInfoSet for ${project.resourceId} has no infos"
    }
  }

  "toDatasetSearchInfo" should {

    "copy data from the ModelDatasetSearchInfo if there's no TSDatasetSearchInfo" in {

      val project   = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val modelInfo = modelDatasetSearchInfoObjects(project).generateOne

      CalculatorInfoSet
        .from(project.identification, modelInfo.some, maybeTSInfo = None)
        .value
        .asDatasetSearchInfo
        .value shouldBe DatasetSearchInfo(
        modelInfo.topmostSameAs,
        modelInfo.name,
        modelInfo.slug,
        modelInfo.createdOrPublished,
        modelInfo.maybeDateModified,
        modelInfo.creators,
        modelInfo.keywords,
        modelInfo.maybeDescription,
        modelInfo.images,
        NonEmptyList.one(modelInfo.link)
      )
    }

    "return None if there's no ModelDatasetSearchInfo" in {

      val project = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val tsInfo  = tsDatasetSearchInfoObjects(project).generateOne

      CalculatorInfoSet
        .from(project.identification, maybeModelInfo = None, tsInfo.some)
        .value
        .asDatasetSearchInfo shouldBe None
    }

    "merge data from the ModelDatasetSearchInfo and TSDatasetSearchInfo if both given" in {

      val project   = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val modelInfo = modelDatasetSearchInfoObjects(project).generateOne
      val tsInfo    = tsDatasetSearchInfoObjects.generateOne.copy(topmostSameAs = modelInfo.topmostSameAs)

      val info =
        CalculatorInfoSet.from(project.identification, modelInfo.some, tsInfo.some).value.asDatasetSearchInfo.value

      info.topmostSameAs      shouldBe modelInfo.topmostSameAs
      info.name               shouldBe modelInfo.name
      info.createdOrPublished shouldBe modelInfo.createdOrPublished
      info.maybeDateModified  shouldBe modelInfo.maybeDateModified
      info.creators           shouldBe modelInfo.creators
      info.keywords           shouldBe modelInfo.keywords
      info.maybeDescription   shouldBe modelInfo.maybeDescription
      info.images             shouldBe modelInfo.images
      info.links.toList         should contain theSameElementsAs (modelInfo.link :: tsInfo.links)
    }

    "use the project link from the model if also exists in TS" in {

      val project   = anyRenkuProjectEntities.map(_.to[entities.Project]).generateOne
      val modelInfo = modelDatasetSearchInfoObjects(project).generateOne
      val tsInfo = {
        val info         = tsDatasetSearchInfoObjects(project.resourceId).generateOne
        val otherTSLinks = linkObjectsGen(modelInfo.topmostSameAs).generateList(min = 1, max = 3)
        info.copy(topmostSameAs = modelInfo.topmostSameAs, links = info.links ::: otherTSLinks)
      }

      val info =
        CalculatorInfoSet.from(project.identification, modelInfo.some, tsInfo.some).value.asDatasetSearchInfo.value

      info.topmostSameAs      shouldBe modelInfo.topmostSameAs
      info.name               shouldBe modelInfo.name
      info.createdOrPublished shouldBe modelInfo.createdOrPublished
      info.maybeDateModified  shouldBe modelInfo.maybeDateModified
      info.creators           shouldBe modelInfo.creators
      info.keywords           shouldBe modelInfo.keywords
      info.maybeDescription   shouldBe modelInfo.maybeDescription
      info.images             shouldBe modelInfo.images
      info.links.toList         should contain theSameElementsAs (
        modelInfo.link :: tsInfo.links.filterNot(_.projectId == project.resourceId)
      )
    }
  }

  "show" should {

    "return String containing project id and slug along with model and TS search info" in {
      forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
        val modelInfo = modelDatasetSearchInfoObjects(withLinkTo = project).generateOne

        val maybeTSInfo = tsDatasetSearchInfoObjects(
          withLinkTo = project.resourceId,
          and = projectResourceIds.generateList(): _*
        ).generateSome.map(_.copy(topmostSameAs = modelInfo.topmostSameAs))

        val infoSet = CalculatorInfoSet.from(project.identification, modelInfo.some, maybeTSInfo).value

        infoSet.show shouldBe List(
          project.identification.show.some,
          show"modelInfo = [${searchIntoToString(modelInfo)}]".some,
          maybeTSInfo.map(tsi => show"tsInfo = [$tsi]")
        ).flatten.mkString(", ")
      }
    }
  }

  private def searchIntoToString(info: ModelDatasetSearchInfo) = List(
    show"topmostSameAs = ${info.topmostSameAs}",
    show"name = ${info.name}",
    show"slug = ${info.slug}",
    show"visibility = ${info.link.visibility}",
    show"link = ${info.link}"
  ).mkString(", ")
}

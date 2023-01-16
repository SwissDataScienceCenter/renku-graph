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
package commands

import Generators._
import cats.syntax.all._
import io.renku.entities.searchgraphs.commands.CalculatorInfoSet.show
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model.testentities._
import io.renku.graph.model.{entities, projects}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CalculatorInfoSetSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "from" should {

    "create CalculatorInfoSet if there's no TS Search Info" in {

      val project        = anyRenkuProjectEntities(anyVisibility).map(_.to[entities.Project]).generateOne
      val modelInfo      = searchInfoObjectsGen(project.resourceId).generateOne
      val tsVisibilities = Map.empty[projects.ResourceId, projects.Visibility]

      val Right(infoSet) = CalculatorInfoSet.from(project, modelInfo.some, None, tsVisibilities)

      infoSet shouldBe CalculatorInfoSet.ModelInfoOnly(project, modelInfo)
    }

    "create CalculatorInfoSet if there's no model Search Info" in {

      val project        = anyRenkuProjectEntities(anyVisibility).map(_.to[entities.Project]).generateOne
      val tsInfo         = searchInfoObjectsGen(project.resourceId).generateOne
      val tsVisibilities = Map(project.resourceId -> project.visibility)

      val Right(infoSet) = CalculatorInfoSet.from(project, None, tsInfo.some, tsVisibilities)

      infoSet shouldBe CalculatorInfoSet.TSInfoOnly(project, tsInfo, tsVisibilities)
    }

    "create CalculatorInfoSet if there are both Search Infos" in {

      val project        = anyRenkuProjectEntities(anyVisibility).map(_.to[entities.Project]).generateOne
      val modelInfo      = searchInfoObjectsGen(project.resourceId).generateOne
      val tsInfo         = searchInfoObjectsGen(project.resourceId).generateOne
      val tsVisibilities = Map(project.resourceId -> project.visibility)

      val Right(infoSet) = CalculatorInfoSet.from(project, modelInfo.some, tsInfo.some, tsVisibilities)

      infoSet shouldBe CalculatorInfoSet.AllInfos(project, modelInfo, tsInfo, tsVisibilities)
    }

    "fail if there's model Search Info with multiple Links" in {

      val project        = anyRenkuProjectEntities(anyVisibility).map(_.to[entities.Project]).generateOne
      val modelInfo      = searchInfoObjectsGen(project.resourceId, projectResourceIds.generateOne).generateOne
      val tsVisibilities = Map.empty[projects.ResourceId, projects.Visibility]

      val result = CalculatorInfoSet.from(project, modelInfo.some, None, tsVisibilities)

      result shouldBe a[Left[_, _]]
      result.leftMap(_.getMessage) shouldBe
        show"CalculatorInfoSet for ${project.resourceId} is linked to many projects".asLeft
    }

    "fail if there's model Search Info pointing to some other Project" in {

      val project        = anyRenkuProjectEntities(anyVisibility).map(_.to[entities.Project]).generateOne
      val modelInfo      = searchInfoObjectsGen(projectResourceIds.generateOne).generateOne
      val tsVisibilities = Map.empty[projects.ResourceId, projects.Visibility]

      val result = CalculatorInfoSet.from(project, modelInfo.some, None, tsVisibilities)

      result shouldBe a[Left[_, _]]
      result.leftMap(_.getMessage) shouldBe
        show"CalculatorInfoSet for ${project.resourceId} has model linked to ${modelInfo.links.head.projectId}".asLeft
    }

    "fail if there is neither model nor ts info" in {

      val project = anyRenkuProjectEntities(anyVisibility).map(_.to[entities.Project]).generateOne

      val result = CalculatorInfoSet.from(project, None, None, Map.empty)

      result                       shouldBe a[Left[_, _]]
      result.leftMap(_.getMessage) shouldBe show"CalculatorInfoSet for ${project.resourceId} has no infos".asLeft
    }
  }

  "show" should {

    "return String containing project id and path along with model and TS search info" in {
      forAll(anyProjectEntities.map(_.to[entities.Project])) { project =>
        val maybeModelInfo = searchInfoObjectsGen(withLinkTo = project.resourceId).generateSome

        val maybeTSInfo = searchInfoObjectsGen(
          withLinkTo = project.resourceId,
          and = projectResourceIds.generateList(): _*
        ).generateSome

        val tsVisibilities = maybeTSInfo.map(_ => Map(project.resourceId -> project.visibility)).getOrElse(Map.empty)

        val infoSet =
          CalculatorInfoSet.from(project, maybeModelInfo, maybeTSInfo, tsVisibilities).fold(throw _, identity)

        infoSet.show shouldBe List(
          show"projectId = ${infoSet.project.resourceId}".some,
          show"projectPath = ${infoSet.project.path}".some,
          maybeModelInfo.map(mi => show"modelInfo = [${searchIntoToString(mi)}]"),
          maybeTSInfo.map(tsi => show"tsInfo = [${searchIntoToString(tsi)}]")
        ).flatten.mkString(", ")
      }
    }
  }

  private def searchIntoToString(info: SearchInfo) = List(
    show"topmostSameAs = ${info.topmostSameAs}",
    show"name = ${info.name}",
    show"visibility = ${info.visibility}",
    show"links = [${info.links.map(link => show"projectId = ${link.projectId}, datasetId = ${link.datasetId}").intercalate("; ")}}]"
  ).mkString(", ")
}

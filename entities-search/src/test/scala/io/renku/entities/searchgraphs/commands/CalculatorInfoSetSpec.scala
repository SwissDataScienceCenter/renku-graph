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

import CalculatorInfoSetGenerators._
import Generators._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CalculatorInfoSetSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "show" should {

    "return String containing project id and path along with model and TS search info" in {
      forAll(calculatorInfoSets) { someInfoSet =>
        val maybeModelInfo = searchInfoObjectsGen(withLinkTo =
          someInfoSet.project.resourceId -> someInfoSet.project.visibility
        ).generateOption

        val maybeTsInfo = searchInfoObjectsGen(
          withLinkTo = someInfoSet.project.resourceId -> someInfoSet.project.visibility,
          and = projectResourceIds.generateList().map(_ -> someInfoSet.project.visibility): _*
        ).generateOption

        val infoSet = someInfoSet.copy(maybeModelInfo = maybeModelInfo, maybeTSInfo = maybeTsInfo)

        infoSet.show shouldBe List(
          show"projectId = ${infoSet.project.resourceId}".some,
          show"projectPath = ${infoSet.project.path}".some,
          infoSet.maybeModelInfo.map(mi => show"modelInfo = [${searchIntoToString(mi)}]"),
          infoSet.maybeTSInfo.map(tsi => show"tsInfo = [${searchIntoToString(tsi)}]")
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

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
import Encoders._
import Generators._
import UpdateCommand._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class CommandsCalculatorSpec extends AnyWordSpec with should.Matchers {

  private val calculateCommands = CommandsCalculator.calculateCommands[Try]

  "calculateCommands" should {

    "return no commands " +
      "when DS exists on both TS and the Project" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId).generateSome

        val tsInfo = searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId,
                                          and = projectResourceIds.generateList(): _*
        ).generateSome

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo, maybeTSInfo = tsInfo)

        calculateCommands(infoSet) shouldBe Nil.pure[Try]
      }

    "create Inserts for the Project found DS " +
      "when it does not exist the TS" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = None)

        calculateCommands(infoSet) shouldBe modelInfo.asQuads(searchInfoEncoder).map(Insert).toList.pure[Try]
      }

    "create Deletes for the DS found in the TS " +
      "when it does not exist on the Project" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val tsInfo = searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = None, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) shouldBe tsInfo.asQuads.map(Delete).toList.pure[Try]
      }

    "create Deletes for the Project-DS association only " +
      "when a DS found for the Project in the TS but it does not exist on the Project anymore (still exists on other Projects)" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val otherLinkedProjects = projectResourceIds.generateNonEmptyList().toList
        val tsInfo =
          searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId, and = otherLinkedProjects: _*).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = None, maybeTSInfo = tsInfo.some)

        val expectedLinkToDelete = tsInfo.links
          .find(_.projectId == someInfoSet.project.resourceId)
          .getOrElse(fail("There supposed to be a link to delete"))

        calculateCommands(infoSet).map(_.toSet) shouldBe (expectedLinkToDelete.asQuads + DatasetsQuad(
          tsInfo.topmostSameAs,
          SearchInfoOntology.linkProperty,
          expectedLinkToDelete.resourceId.asEntityId
        )).map(Delete).pure[Try]
      }

    "fail for when input data in illegal state" in {

      val someInfoSet = calculatorInfoSets.generateOne

      val modelInfo = searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId).generateSome

      val tsInfo = searchInfoObjectsGen(withLinkFor = projectResourceIds.generateOne).generateSome

      val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo, maybeTSInfo = tsInfo)

      val Failure(exception) = calculateCommands(infoSet).map(_.toSet)

      exception.getMessage shouldBe show"Cannot calculate update commands for $infoSet"
    }
  }
}

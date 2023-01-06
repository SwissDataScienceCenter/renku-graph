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
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CommandsCalculatorSpec extends AnyWordSpec with should.Matchers {

  import CommandsCalculator._

  "calculateCommands" should {

    "create Inserts for the project derived DS search info " +
      "when no DS in the TS" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val modelInfo = searchInfoObjects(withLinkFor = someInfoSet.project.resourceId).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = modelInfo.some, maybeTSInfo = None)

        calculateCommands(infoSet) shouldBe modelInfo.asQuads(searchInfoEncoder).map(Insert).toList
      }

    "create Deletes for the search info from the TS " +
      "when the TS info has solely the current project and the current project does not have the DS anymore" in {

        val someInfoSet = calculatorInfoSets.generateOne

        val tsInfo = searchInfoObjects(withLinkFor = someInfoSet.project.resourceId).generateOne

        val infoSet = someInfoSet.copy(maybeModelInfo = None, maybeTSInfo = tsInfo.some)

        calculateCommands(infoSet) shouldBe tsInfo.asQuads.map(Delete).toList
      }
  }
}

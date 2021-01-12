/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.awaitinggeneration.triplescuration

import CurationGenerators._

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.triplesgenerator.events.awaitinggeneration.triplescuration.CuratedTriples.CurationUpdatesGroup
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.util.Try

class CuratedTriplesSpec extends AnyWordSpec with ScalaCheckPropertyChecks with MockFactory with should.Matchers {

  "generateUpdates" should {

    "call the given query creation function" in {
      forAll { updateFunction: CurationUpdatesGroup[Try] =>
        updateFunction.generateUpdates() shouldBe updateFunction.queryGenerator()
      }
    }
  }
}

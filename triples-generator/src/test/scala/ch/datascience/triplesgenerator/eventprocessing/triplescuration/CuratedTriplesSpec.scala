/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration

import CurationGenerators._
import ch.datascience.generators.CommonGraphGenerators.jsonLDTriples
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers._
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CuratedTriplesSpec extends AnyWordSpec with ScalaCheckPropertyChecks with MockFactory with should.Matchers {

  "addUpdates" should {

    "append the given updates to what's already in the curated triples" in {
      forAll { (curatedTriples: CuratedTriples, updates: List[Update]) =>
        curatedTriples.addUpdates(updates) shouldBe CuratedTriples(
          curatedTriples.triples,
          curatedTriples.updates ++ updates
        )
      }
    }
  }

  "transformTriples" should {

    "transform triples with the given function" in {
      val curatedTriples = curatedTriplesObjects.generateOne

      val transformedTriples = jsonLDTriples.generateOne
      val f                  = mockFunction[JsonLDTriples, JsonLDTriples]
      f.expects(curatedTriples.triples).returning(transformedTriples)

      curatedTriples.transformTriples(f).triples shouldBe transformedTriples
    }
  }
}

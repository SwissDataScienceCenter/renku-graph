/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import cats.MonadError
import cats.implicits._
import ch.datascience.generators.CommonGraphGenerators.jsonLDTriples
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.Matchers._
import org.scalatest.WordSpec

import scala.util.Try

class TriplesCuratorSpec extends WordSpec with MockFactory {

  "curate" should {

    "pass the given triples through the curation step and return the final results" in new TestCase {

      val updatedTriples = curatedTriplesObjects().generateOne
      (personDetailsUpdater.curate _)
        .expects(CuratedTriples(triples, updates = Nil))
        .returning(context.pure(updatedTriples))

      curator.curate(triples) shouldBe context.pure(updatedTriples)
    }

    "pass the curation step failure when it happened" in new TestCase {

      val exception = exceptions.generateOne
      (personDetailsUpdater.curate _)
        .expects(CuratedTriples(triples, updates = Nil))
        .returning(context.raiseError(exception))

      curator.curate(triples) shouldBe context.raiseError(exception)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val triples = jsonLDTriples.generateOne

    class TryPersonDetailsUpdater extends PersonDetailsUpdater[Try]
    val personDetailsUpdater = mock[TryPersonDetailsUpdater]
    val curator              = new TriplesCurator[Try](personDetailsUpdater)
  }
}

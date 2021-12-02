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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.persondetails

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGPersonFinderSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "find" should {

    "find a Person by its resourceId" in new TestCase {
      val person = personEntities().generateOne.to[entities.Person]

      loadToStore(person)

      finder.find(person).unsafeRunSync() shouldBe Some(person)
    }

    "return no Person if it doesn't exist" in new TestCase {
      finder
        .find(personEntities().generateOne.to[entities.Person])
        .unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())
    val finder               = new KGPersonFinderImpl(rdfStoreConfig, timeRecorder)
  }
}

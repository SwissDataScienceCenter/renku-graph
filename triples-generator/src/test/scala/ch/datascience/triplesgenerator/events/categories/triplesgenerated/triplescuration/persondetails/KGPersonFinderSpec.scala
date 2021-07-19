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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGPersonFinderSpec extends AnyWordSpec with InMemoryRdfStore with should.Matchers {

  "find" should {

    "find a Person by gitLabId when exists" in new TestCase {
      val person = personEntities(withGitLabId, withoutEmail).generateOne.to[entities.Person]

      loadToStore(person)

      finder
        .find(person.copy(resourceId = userResourceIds.generateOne))
        .unsafeRunSync() shouldBe Some(person)
    }

    "find a Person by email when exists" in new TestCase {
      val person = personEntities(withoutGitLabId, withEmail).generateOne.to[entities.Person]

      loadToStore(person)

      finder.find(person.copy(resourceId = userResourceIds.generateOne)).unsafeRunSync() shouldBe Some(person)
    }

    "find a Person by resourceId if there's no gitLabId or email" in new TestCase {
      val person = personEntities(withoutGitLabId, withoutEmail).generateOne.to[entities.Person]

      loadToStore(person)

      finder.find(person).unsafeRunSync() shouldBe Some(person)
    }

    "find a Person by email if there's no gitLabId on it" in new TestCase {
      val person = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]

      loadToStore(person)

      finder
        .find(person.copy(maybeGitLabId = None))
        .unsafeRunSync() shouldBe Some(person)
    }

    "find a Person by resourceId if there's no gitLabId or email on it" in new TestCase {
      val person = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]

      loadToStore(person)

      finder
        .find(person.copy(maybeGitLabId = None, maybeEmail = None))
        .unsafeRunSync() shouldBe Some(person)
    }

    "find a Person by email if gitLabIds exist on model and KG but they are different" in new TestCase {
      val person = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]

      loadToStore(person)

      finder
        .find(person.copy(maybeGitLabId = userGitLabIds.generateSome))
        .unsafeRunSync() shouldBe Some(person)
    }

    "find a Person by resourceId if both gitLabIds and emails exist on model and KG but they are different" in new TestCase {
      val person = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]

      loadToStore(person)

      finder
        .find(person.copy(maybeGitLabId = userGitLabIds.generateSome, maybeEmail = userEmails.generateSome))
        .unsafeRunSync() shouldBe Some(person)
    }

    "return no Person if it doesn't exist" in new TestCase {
      finder
        .find(personEntities(withGitLabId, withEmail).generateOne.to[entities.Person])
        .unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val finder               = new KGPersonFinderImpl(rdfStoreConfig, logger, timeRecorder)
  }
}

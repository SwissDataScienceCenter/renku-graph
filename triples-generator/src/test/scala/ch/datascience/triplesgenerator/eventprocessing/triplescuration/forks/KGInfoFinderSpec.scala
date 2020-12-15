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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.IO
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.entities.Person.persons
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.jsonld.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGInfoFinderSpec extends AnyWordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks with should.Matchers {

  "findCreatorId" should {

    "return ResourceId of a Person with the given gitLabId" in new TestCase {
      forAll { gitLabId: users.GitLabId =>
        val person = persons.generateOne.copy(maybeGitLabId = Some(gitLabId))

        loadToStore(person.asJsonLD, persons.generateOne.asJsonLD)

        val Some(resourceId) = finder.findCreatorId(gitLabId).unsafeRunSync()

        findPerson(resourceId) shouldBe Set(person.name.value)

        clearDataset()
      }
    }

    "return no ResourceId if there's no Person with the given gitLabId" in new TestCase {
      finder.findCreatorId(userGitLabIds.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val finder               = new IOKGInfoFinder(rdfStoreConfig, logger, timeRecorder)
  }

  private def findPerson(resourceId: users.ResourceId): Set[String] =
    runQuery(s"""|SELECT ?name
                 |WHERE {
                 |  ${resourceId.showAs[RdfResource]} rdf:type schema:Person;
                 |                                    schema:name ?name
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => row("name"))
      .toSet
}

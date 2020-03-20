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
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.projects.ResourceId
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.interpreters.TestLogger
import ch.datascience.logging.TestExecutionTimeRecorder
import ch.datascience.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder, entities}
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class KGInfoFinderSpec extends WordSpec with InMemoryRdfStore with ScalaCheckPropertyChecks {

  "findProject" should {

    "return details of the project from the KG if it exists" in new TestCase {
      forAll { entitiesProject: entities.Project =>
        loadToStore(entitiesProject.asJsonLD)

        val Some(kgProject) = finder.findProject(entitiesProject.path).unsafeRunSync()

        kgProject.resourceId shouldBe ResourceId(renkuBaseUrl, entitiesProject.path)
        kgProject.maybeParentResourceId shouldBe entitiesProject.maybeParentProject.map(
          parent => ResourceId(renkuBaseUrl, parent.path)
        )
        kgProject.dateCreated        shouldBe entitiesProject.dateCreated
        kgProject.creator.maybeEmail shouldBe entitiesProject.creator.maybeEmail
        kgProject.creator.maybeName  shouldBe entitiesProject.creator.name.some
      }
    }

    "return no details if the project does not exists in the KG" in new TestCase {
      finder
        .findProject(projectPaths.generateOne)
        .unsafeRunSync() shouldBe None
    }
  }

  "findCreatorId" should {

    "return ResourceId of a Person with the given email" in new TestCase {
      forAll { email: Email =>
        val person = entitiesPersons(email.some).generateOne

        loadToStore(person.asJsonLD, entitiesPersons().generateOne.asJsonLD)

        val Some(resourceId) = finder.findCreatorId(email).unsafeRunSync()

        findPerson(resourceId) should contain theSameElementsAs Set(
          person.name.value -> email.value
        )
      }
    }

    "return no ResourceId if there's no Person with the given email" in new TestCase {
      finder.findCreatorId(userEmails.generateOne).unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private val logger       = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder(logger))
    val finder               = new IOKGInfoFinder(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
  }

  private implicit val projectsGen: Gen[entities.Project] = for {
    maybeParent <- entitiesProjects(maybeParentProject = None).toGeneratorOfOptions
    project     <- entitiesProjects(maybeParentProject = maybeParent)
  } yield project

  private def findPerson(resourceId: users.ResourceId): Set[(String, String)] =
    runQuery(s"""|SELECT ?name ?email
                 |WHERE {
                 |  ${resourceId.showAs[RdfResource]} rdf:type schema:Person;
                 |                                    schema:name ?name;
                 |                                    schema:email ?email 
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => (row("name"), row("email")))
      .toSet
}

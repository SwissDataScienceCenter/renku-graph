/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.persons

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.personNames
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{GraphClass, entities, persons}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.SparqlQuery._
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.{OptionValues, Succeeded}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class KGPersonFinderSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers
    with OptionValues {

  "find" should {

    "find a Person by its resourceId" in projectsDSConfig.use { implicit pcc =>
      val person = personEntities().generateOne.to[entities.Person]

      uploadToProjects(person) >>
        (finder find person).asserting(_ shouldBe Some(person))
    }

    "pick-up one of the Person names if there multiple" in projectsDSConfig.use { implicit pcc =>
      val person = personEntities().generateOne.to[entities.Person]

      for {
        _ <- uploadToProjects(person)

        duplicateNames = personNames.generateNonEmptyList().toList
        _ <- duplicateNames.traverse_ { name =>
               insert(Quad(GraphClass.Persons.id, person.resourceId.asEntityId, schema / "name", name.asObject))
             }
        _ <- findNames(person).asserting(_ shouldBe duplicateNames.toSet + person.name)

        found <- finder.find(person).map(_.value)

        _ = found.resourceId       shouldBe person.resourceId
        _ = Set(found.name)          should contain oneElementOf (duplicateNames.toSet + person.name)
        _ = found.maybeEmail       shouldBe person.maybeEmail
        _ = found.maybeAffiliation shouldBe person.maybeAffiliation
      } yield Succeeded
    }

    "return no Person if it doesn't exist" in projectsDSConfig.use { implicit pcc =>
      finder
        .find(personEntities().generateOne.to[entities.Person])
        .asserting(_ shouldBe None)
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def finder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new KGPersonFinderImpl[IO](pcc)
  }

  private def findNames(id: entities.Person)(implicit pcc: ProjectsConnectionConfig): IO[Set[persons.Name]] =
    runSelect(
      SparqlQuery.of(
        "fetch person name",
        Prefixes of schema -> "schema",
        s"""|SELECT ?name 
            |WHERE { 
            |  GRAPH <${GraphClass.Persons.id.show}> {
            |    ${id.resourceId.showAs[RdfResource]} schema:name ?name
            |  }
            |}""".stripMargin
      )
    ).map(_.map(row => persons.Name(row("name"))).toSet)
}

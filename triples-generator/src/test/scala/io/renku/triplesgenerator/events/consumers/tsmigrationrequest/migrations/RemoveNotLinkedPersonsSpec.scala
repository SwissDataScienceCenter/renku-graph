/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class RemoveNotLinkedPersonsSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "run" should {

    "find all Person objects that are not linked to any entity and remove them" in {
      val project = anyRenkuProjectEntities
        .modify(replaceProjectCreator(personEntities.generateSome))
        .modify(replaceMembers(Set.empty))
        .map(_.to[entities.Project])
        .generateOne

      assume(project.maybeCreator.isDefined)

      upload(to = projectsDataset, project)

      val person = personEntities.generateOne.to[entities.Person]
      upload(to = projectsDataset, person)

      assume((project.maybeCreator.map(_.resourceId).toSet + person.resourceId).size > 1)

      findAllPersons shouldBe project.maybeCreator.map(_.resourceId).toSet + person.resourceId

      runUpdate(on = projectsDataset, RemoveNotLinkedPersons.query).unsafeRunSync() shouldBe ()

      findAllPersons shouldBe project.maybeCreator.map(_.resourceId).toSet
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      RemoveNotLinkedPersons[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findAllPersons: Set[persons.ResourceId] = runSelect(
    on = projectsDataset,
    SparqlQuery.of(
      "fetch personId",
      Prefixes of schema -> "schema",
      s"""|SELECT ?personId
          |FROM <${GraphClass.Persons.id}> { 
          |  ?personId a schema:Person 
          |}
          |""".stripMargin
    )
  ).unsafeRunSync()
    .map(row => persons.ResourceId(row("personId")))
    .toSet
}

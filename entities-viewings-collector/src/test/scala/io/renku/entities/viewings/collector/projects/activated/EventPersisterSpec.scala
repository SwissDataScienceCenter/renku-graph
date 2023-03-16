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

package io.renku.entities.viewings.collector.projects.activated

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.{projects, GraphClass}
import io.renku.graph.model.testentities._
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.ProjectActivated
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class EventPersisterSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "persist" should {

    "insert the given ProjectActivated event into the TS " +
      "if there's no event for the project yet" in new TestCase {

        val project = anyProjectEntities.generateOne
        upload(to = projectsDataset, project)

        val event = projectActivatedEvents.generateOne.copy(path = project.path)

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(project.resourceId -> projects.DateViewed(event.dateActivated.value))
      }

    "do nothing if there's already an event for the project in the TS" in new TestCase {

      val project = anyProjectEntities.generateOne
      upload(to = projectsDataset, project)

      val event = projectActivatedEvents.generateOne.copy(path = project.path)

      persister.persist(event).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(project.resourceId -> projects.DateViewed(event.dateActivated.value))

      val otherEvent = event.copy(dateActivated = timestampsNotInTheFuture.generateAs(ProjectActivated.DateActivated))

      persister.persist(otherEvent).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(project.resourceId -> projects.DateViewed(event.dateActivated.value))
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val persister = new EventPersisterImpl[IO](TSClient[IO](projectsDSConnectionInfo))
  }

  private def findAllViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find project viewing",
        Prefixes of renku -> "renku",
        s"""|SELECT ?id ?date
            |FROM ${GraphClass.ProjectViewedTimes.id.asSparql.sparql} {
            |  ?id renku:dateViewed ?date.
            |}
            |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row => projects.ResourceId(row("id")) -> projects.DateViewed(Instant.parse(row("date"))))
      .toSet
}

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

package io.renku.entities.viewings.collector.projects
package activated

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.projects
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.ProjectActivated
import io.renku.triplesstore._
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class EventPersisterSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with TestSearchInfoDatasets
    with EventPersisterSpecTools
    with AsyncMockFactory
    with should.Matchers {

  "persist" should {

    "insert the given ProjectActivated event into the TS and run the deduplication query " +
      "if there's no event for the project yet" in projectsDSConfig.use { implicit pcc =>
        val project = anyProjectEntities.generateOne
        for {
          _ <- provisionTestProject(project)

          event = projectActivatedEvents.generateOne.copy(slug = project.slug)

          _ = givenEventDeduplication(project, returning = ().pure[IO])

          _ <- persister.persist(event).assertNoException

          _ <- findAllViewings.asserting(
                 _ shouldBe Set(project.resourceId -> projects.DateViewed(event.dateActivated.value))
               )
        } yield Succeeded
      }

    "do nothing if there's already an event for the project in the TS" in projectsDSConfig.use { implicit pcc =>
      val project = anyProjectEntities.generateOne
      for {
        _ <- provisionTestProject(project)

        event = projectActivatedEvents.generateOne.copy(slug = project.slug)

        _ = givenEventDeduplication(project, returning = ().pure[IO])

        _ <- persister.persist(event).assertNoException

        _ <- findAllViewings.asserting(
               _ shouldBe Set(project.resourceId -> projects.DateViewed(event.dateActivated.value))
             )

        otherEvent = event.copy(dateActivated = timestampsNotInTheFuture.generateAs(ProjectActivated.DateActivated))

        _ <- persister.persist(otherEvent).assertNoException

        _ <- findAllViewings.asserting(
               _ shouldBe Set(project.resourceId -> projects.DateViewed(event.dateActivated.value))
             )
      } yield Succeeded
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private val eventDeduplicator = mock[EventDeduplicator[IO]]
  private def persister(implicit pcc: ProjectsConnectionConfig) =
    new EventPersisterImpl[IO](tsClient, eventDeduplicator)

  private def givenEventDeduplication(project: Project, returning: IO[Unit]) =
    (eventDeduplicator.deduplicate _)
      .expects(project.resourceId)
      .returning(returning)
}

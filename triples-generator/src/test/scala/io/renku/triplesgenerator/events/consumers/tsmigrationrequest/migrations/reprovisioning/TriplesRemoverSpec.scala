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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.reprovisioning

import cats.effect.IO
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.graph.model.{entities, persons, GraphClass}
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.OptionValues

class TriplesRemoverSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "removeAllTriples" should {

    "remove all graphs from the projects DS except from the renku:ProjectViewedTime and renku:PersonViewing" in new TestCase {

      val someProject = anyRenkuProjectEntities
        .map(replaceProjectCreator(personEntities(personGitLabIds.generateSome).generateSome))
        .generateOne
        .to[entities.RenkuProject]
      upload(to = projectsDataset, someProject, anyRenkuProjectEntities.generateOne.to[entities.RenkuProject])
      insertViewTime(someProject, someProject.maybeCreator.value.resourceId)

      triplesCount(on = projectsDataset).unsafeRunSync() should be > 0L

      val projectViewedGraphTriples = triplesCount(projectsDataset, GraphClass.ProjectViewedTimes.id).unsafeRunSync()
      projectViewedGraphTriples should be > 0L
      val personViewingsGraphTriples = triplesCount(projectsDataset, GraphClass.PersonViewings.id).unsafeRunSync()
      personViewingsGraphTriples should be > 0L

      triplesRemover.removeAllTriples().unsafeRunSync() shouldBe ()

      triplesCount(on = projectsDataset).unsafeRunSync() shouldBe projectViewedGraphTriples + personViewingsGraphTriples

      triplesCount(projectsDataset, GraphClass.ProjectViewedTimes.id).unsafeRunSync() shouldBe projectViewedGraphTriples
      triplesCount(projectsDataset, GraphClass.PersonViewings.id).unsafeRunSync() shouldBe personViewingsGraphTriples
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private implicit val tr: SparqlQueryTimeRecorder[IO] =
      new SparqlQueryTimeRecorder[IO](TestExecutionTimeRecorder[IO]())
    val triplesRemover = new TriplesRemoverImpl[IO](projectsDSConnectionInfo)
  }

  private def insertViewTime(project: entities.Project, viewerId: persons.ResourceId): Unit = {

    insert(
      to = projectsDataset,
      Quad(GraphClass.ProjectViewedTimes.id,
           project.resourceId.asEntityId,
           renku / "prop",
           nonEmptyStrings().generateOne.asTripleObject
      )
    )

    insert(
      to = projectsDataset,
      Quad(GraphClass.PersonViewings.id,
           viewerId.asEntityId,
           renku / "prop",
           nonEmptyStrings().generateOne.asTripleObject
      )
    )
  }
}

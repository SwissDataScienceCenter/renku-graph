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
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits.GenOps
import io.renku.generators.Generators.nonEmptyStrings
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, persons}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}

class TriplesRemoverSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with GraphJenaSpec
    with should.Matchers
    with OptionValues {

  "removeAllTriples" should {

    "remove all graphs from the projects DS except from the renku:ProjectViewedTime and renku:PersonViewing" in projectsDSConfig
      .use { implicit pcc =>
        val someProject = anyRenkuProjectEntities
          .map(replaceProjectCreator(personEntities(personGitLabIds.generateSome).generateSome))
          .generateOne
          .to[entities.RenkuProject]

        for {
          _ <- uploadToProjects(someProject, anyRenkuProjectEntities.generateOne.to[entities.RenkuProject])
          _ <- insertViewTime(someProject, someProject.maybeCreator.value.resourceId)

          _ <- triplesCount.asserting(_ should be > 0L)

          projectViewedGraphTriples <- triplesCount(GraphClass.ProjectViewedTimes.id)
          _ = projectViewedGraphTriples should be > 0L
          personViewingsGraphTriples <- triplesCount(GraphClass.PersonViewings.id)
          _ = personViewingsGraphTriples should be > 0L

          _ <- triplesRemover.removeAllTriples().assertNoException

          _ <- triplesCount.asserting(_ shouldBe projectViewedGraphTriples + personViewingsGraphTriples)

          _ <- triplesCount(GraphClass.ProjectViewedTimes.id).asserting(_ shouldBe projectViewedGraphTriples)
          _ <- triplesCount(GraphClass.PersonViewings.id).asserting(_ shouldBe personViewingsGraphTriples)
        } yield Succeeded
      }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def triplesRemover(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new TriplesRemoverImpl[IO](pcc)
  }

  private def insertViewTime(project: entities.Project, viewerId: persons.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[Unit] =
    insert(
      Quad(GraphClass.ProjectViewedTimes.id,
           project.resourceId.asEntityId,
           renku / "prop",
           nonEmptyStrings().generateOne.asTripleObject
      )
    ) >>
      insert(
        Quad(GraphClass.PersonViewings.id,
             viewerId.asEntityId,
             renku / "prop",
             nonEmptyStrings().generateOne.asTripleObject
        )
      )
}

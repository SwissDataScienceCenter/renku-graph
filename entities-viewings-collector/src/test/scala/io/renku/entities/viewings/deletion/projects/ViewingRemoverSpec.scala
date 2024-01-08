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

package io.renku.entities.viewings
package deletion.projects

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.searchgraphs.TestSearchInfoDatasets
import io.renku.entities.viewings.collector.projects.viewed.EventPersister
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.fixed
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.api.events.Generators._
import io.renku.triplesgenerator.api.events.{ProjectViewingDeletion, UserId}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{OptionValues, Succeeded}

class ViewingRemoverSpec
    extends AsyncWordSpec
    with AsyncIOSpec
    with ViewingsCollectorJenaSpec
    with TestSearchInfoDatasets
    with should.Matchers
    with OptionValues {

  "removeViewing" should {

    "remove the relevant triples from the ProjectViewedTime and PersonViewing graphs" in projectsDSConfig.use {
      implicit pcc =>
        val userId  = userIds.generateOne
        val project = generateProjectWithCreator(userId)
        for {
          _ <- insertViewing(project, userId)

          otherProject = generateProjectWithCreator(userId)
          _ <- insertViewing(otherProject, userId)

          personId = otherProject.maybeCreator.value.resourceId
          _ <- findProjectsWithViewings.asserting(_ shouldBe Set(project.resourceId, otherProject.resourceId))
          _ <- findPersonViewings.asserting(
                 _ shouldBe Set(personId -> project.resourceId, personId -> otherProject.resourceId)
               )

          event = ProjectViewingDeletion(project.slug)

          _ <- remover.removeViewing(event).assertNoException

          _ <- findProjectsWithViewings.asserting(_ shouldBe Set(otherProject.resourceId))
          _ <- findPersonViewings.asserting(_ shouldBe Set(personId -> otherProject.resourceId))
        } yield Succeeded
    }

    "remove all person data from the PersonViewing graph " +
      "if he had viewings only from the project that is gone" in projectsDSConfig.use { implicit pcc =>
        val userId  = userIds.generateOne
        val project = generateProjectWithCreator(userId)
        for {
          _ <- insertViewing(project, userId)

          otherUserId  = userIds.generateOne
          otherProject = generateProjectWithCreator(otherUserId)
          _ <- insertViewing(otherProject, otherUserId)

          personId      = project.maybeCreator.value.resourceId
          otherPersonId = otherProject.maybeCreator.value.resourceId
          _ <- findProjectsWithViewings.asserting(_ shouldBe Set(project.resourceId, otherProject.resourceId))
          _ <- findPersonViewings.asserting(
                 _ shouldBe Set(personId -> project.resourceId, otherPersonId -> otherProject.resourceId)
               )

          event = ProjectViewingDeletion(project.slug)

          _ <- remover.removeViewing(event).assertNoException

          _ <- findProjectsWithViewings.asserting(_ shouldBe Set(otherProject.resourceId))
          _ <- findPersonViewings.asserting(_ shouldBe Set(otherPersonId -> otherProject.resourceId))
          _ <- countPersonViewingsTriples(personId).asserting(_ shouldBe 0)
        } yield Succeeded
      }

    "do nothing if there's no project with the given slug" in projectsDSConfig.use { implicit pcc =>
      for {
        _ <- findProjectsWithViewings.asserting(_ shouldBe Set.empty)

        event = ProjectViewingDeletion(projectSlugs.generateOne)

        _ <- remover.removeViewing(event).assertNoException

        _ <- findProjectsWithViewings.asserting(_ shouldBe Set.empty)
      } yield Succeeded
    }

    "do nothing if there are no triples in the PersonViewing graph for the given slug" in projectsDSConfig.use {
      implicit pcc =>
        val userId  = userIds.generateOne
        val project = generateProjectWithCreator(userId)

        for {
          _ <- provisionTestProject(project)

          projectViewedEvent = projectViewedEvents.generateOne.copy(slug = project.slug, maybeUserId = None)

          _ <- eventPersister.persist(projectViewedEvent).assertNoException

          _ <- findProjectsWithViewings.asserting(_ shouldBe Set(project.resourceId))
          _ <- findPersonViewings.asserting(_ shouldBe Set.empty)

          viewingDeletionEvent = ProjectViewingDeletion(project.slug)

          _ <- remover.removeViewing(viewingDeletionEvent).assertNoException

          _ <- findProjectsWithViewings.asserting(_ shouldBe Set.empty)
          _ <- findPersonViewings.asserting(_ shouldBe Set.empty)
          _ <- countPersonViewingsTriples(project.maybeCreator.value.resourceId).asserting(_ shouldBe 0)
        } yield Succeeded
    }
  }

  implicit val ioLogger: TestLogger[IO] = TestLogger[IO]()
  private def remover(implicit pcc: ProjectsConnectionConfig) = new ViewingRemoverImpl[IO](tsClient)

  private def eventPersister(implicit pcc: ProjectsConnectionConfig) = EventPersister[IO](tsClient)

  private def insertViewing(project: Project, userId: UserId)(implicit pcc: ProjectsConnectionConfig) =
    for {
      _ <- provisionTestProject(project)

      event = projectViewedEvents.generateOne.copy(slug = project.slug, maybeUserId = userId.some)
      _ <- eventPersister.persist(event).assertNoException
    } yield Succeeded

  private def generateProjectWithCreator(userId: UserId) = {

    val creator = userId
      .fold(
        glId => personEntities(maybeGitLabIds = fixed(glId.some)).map(removeOrcidId),
        email => personEntities(withoutGitLabId, maybeEmails = fixed(email.some)).map(removeOrcidId)
      )
      .generateSome

    anyProjectEntities
      .map(replaceProjectCreator(creator))
      .generateOne
  }

  private def findProjectsWithViewings(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "test find project viewing",
        Prefixes of renku -> "renku",
        sparql"""|SELECT DISTINCT ?id
                 |FROM ${GraphClass.ProjectViewedTimes.id} {
                 |  ?id a renku:ProjectViewedTime
                 |}
                 |""".stripMargin
      )
    ).map(_.map(row => projects.ResourceId(row("id"))).toSet)

  private def findPersonViewings(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "test find person viewing",
        Prefixes of renku -> "renku",
        sparql"""|SELECT DISTINCT ?userId ?projectId
                 |FROM ${GraphClass.PersonViewings.id} {
                 |  ?userId a renku:PersonViewing;
                 |          renku:viewedProject ?viewingId.
                 |  ?viewingId renku:project ?projectId.
                 |}
                 |""".stripMargin
      )
    ).map(_.map(row => persons.ResourceId(row("userId")) -> projects.ResourceId(row("projectId"))).toSet)

  private def countPersonViewingsTriples(personId: persons.ResourceId)(implicit pcc: ProjectsConnectionConfig) =
    runSelect(
      SparqlQuery.of(
        "test find person viewing triples count",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?s ?p ?o
                 |FROM ${GraphClass.PersonViewings.id} {
                 |  BIND(${personId.asEntityId} AS ?s)
                 |  ?s ?p ?o
                 |}
                 |""".stripMargin
      )
    ).map(_.size)
}

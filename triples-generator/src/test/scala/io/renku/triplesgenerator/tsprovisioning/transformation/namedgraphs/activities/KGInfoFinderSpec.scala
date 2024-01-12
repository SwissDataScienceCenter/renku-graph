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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.activities

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.functor._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model.entities.{ActivityLens, EntityFunctions}
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, activities, entities}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLDEncoder, NamedGraph}
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import monocle.Lens
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class KGInfoFinderSpec extends AsyncWordSpec with AsyncIOSpec with TriplesGeneratorJenaSpec with should.Matchers {

  "findActivityAuthors" should {

    "return activity author's resourceIds" in projectsDSConfig.use { implicit pcc =>
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      for {
        _ <- uploadToProjects(project)

        person = personEntities.generateOne.to[entities.Person]
        _ <- uploadToProjects {
               implicit val enc: JsonLDEncoder[entities.Person] =
                 EntityFunctions[entities.Person].encoder(GraphClass.Persons)
               NamedGraph.fromJsonLDsUnsafe(GraphClass.Persons.id, person.asJsonLD)
             }
        _ <- insert(
               Quad(GraphClass.Project.id(project.resourceId),
                    activity.resourceId.asEntityId,
                    prov / "wasAssociatedWith",
                    person.resourceId.asEntityId
               )
             )

        _ <- kgInfoFinder
               .findActivityAuthors(project.resourceId, activity.resourceId)
               .asserting(_ shouldBe Set(activity.author.resourceId, person.resourceId))
      } yield Succeeded
    }

    "return no author if there's no Activity with the given id" in projectsDSConfig.use { implicit pcc =>
      kgInfoFinder
        .findActivityAuthors(projectResourceIds.generateOne, activities.ResourceId(httpUrls().generateOne))
        .asserting(_ shouldBe Set.empty)
    }
  }

  "findAssociationPersonAgents" should {

    "return activity association person agent resourceIds" in projectsDSConfig.use { implicit pcc =>
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()).map(toAssociationPersonAgent))
        .generateOne
        .to[entities.RenkuProject]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      for {
        _ <- uploadToProjects(project)

        person = personEntities.generateOne.to[entities.Person]
        updatedAgentActivity = ActivityLens.activityAssociationAgent.modify(
                                 _.requireRight("Association.WithPersonAgent expected").as(person)
                               )(activity)

        updatedProject = projectLens.modify(_ => List(updatedAgentActivity))(project)
        _ <- uploadToProjects(updatedProject)

        originalAgent =
          ActivityLens.activityAssociationAgent.get(activity).toOption.getOrElse(fail("expected Person agent"))

        _ <- kgInfoFinder
               .findAssociationPersonAgents(project.resourceId, activity.resourceId)
               .asserting(_ shouldBe Set(originalAgent.resourceId, person.resourceId))
      } yield Succeeded
    }

    "return no agent if there's no Activity with the given id" in projectsDSConfig.use { implicit pcc =>
      kgInfoFinder
        .findAssociationPersonAgents(projectResourceIds.generateOne, activities.ResourceId(httpUrls().generateOne))
        .asserting(_ shouldBe Set.empty)
    }

    "return no agent if there's association with SoftwareAgent agent" in projectsDSConfig.use { implicit pcc =>
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      uploadToProjects(project) >>
        kgInfoFinder
          .findAssociationPersonAgents(project.resourceId, activity.resourceId)
          .asserting(_ shouldBe Set.empty)
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def kgInfoFinder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    new KGInfoFinderImpl[IO](pcc)
  }

  private lazy val projectLens = Lens[entities.RenkuProject, List[entities.Activity]](_.activities) { a =>
    {
      case p: entities.RenkuProject.WithParent    => p.copy(activities = a)
      case p: entities.RenkuProject.WithoutParent => p.copy(activities = a)
    }
  }

  private final implicit class EitherAssertions[A, B](eab: Either[A, B]) {
    def requireRight(message: String): Either[A, B] =
      eab.filterOrElse(_ => true, fail(message))
  }
}

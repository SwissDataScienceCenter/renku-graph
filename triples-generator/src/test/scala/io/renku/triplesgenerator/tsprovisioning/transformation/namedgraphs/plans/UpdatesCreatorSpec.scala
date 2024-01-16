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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.plans

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model._
import io.renku.graph.model.entities.ProjectLens._
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{GraphJenaSpec, ProjectsConnectionConfig, SparqlQuery}
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class UpdatesCreatorSpec extends AsyncWordSpec with AsyncIOSpec with GraphJenaSpec with should.Matchers {

  "queriesDeletingDate" should {

    "prepare delete query if the new Plan has different dateCreated that found in the TS" in projectsDSConfig.use {
      implicit pcc =>
        val project = anyRenkuProjectEntities
          .withActivities(activityEntities(stepPlanEntities()))
          .map(_.to[entities.RenkuProject])
          .generateOne

        for {
          _ <- uploadToProjects(project)

          plan = collectStepPlans(project.plans).headOption.getOrElse(fail("Expected plan"))

          _ <- findPlanDateCreated(project.resourceId, plan.resourceId).asserting(_ shouldBe List(plan.dateCreated))

          _ <- runUpdates {
                 UpdatesCreator.queriesDeletingDate(
                   project.resourceId,
                   plan,
                   timestampsNotInTheFuture.toGeneratorOf(plans.DateCreated).generateFixedSizeList(ofSize = 1)
                 )
               }

          _ <- findPlanDateCreated(project.resourceId, plan.resourceId).asserting(_ shouldBe List.empty)
        } yield Succeeded
    }

    "prepare delete query if there are multiple dates found in the TS" in projectsDSConfig.use { implicit pcc =>
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      for {
        _ <- uploadToProjects(project)

        plan = collectStepPlans(project.plans).headOption.getOrElse(fail("Expected plan"))

        _ <- findPlanDateCreated(project.resourceId, plan.resourceId).asserting(_ shouldBe List(plan.dateCreated))

        _ <- runUpdates {
               UpdatesCreator.queriesDeletingDate(
                 project.resourceId,
                 plan,
                 timestampsNotInTheFuture.toGeneratorOf(plans.DateCreated).generateList(min = 2)
               )
             }

        _ <- findPlanDateCreated(project.resourceId, plan.resourceId).asserting(_ shouldBe List.empty)
      } yield Succeeded
    }

    "do nothing if there's no date set for the Plan in the TS" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val plan = collectStepPlans(project.plans).headOption.getOrElse(fail("Expected plan"))

      UpdatesCreator.queriesDeletingDate(project.resourceId, plan, tsCreatedDates = Nil) shouldBe Nil
    }

    "prepare no queries if there's no change in Plan dateCreated" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val plan = collectStepPlans(project.plans).headOption.getOrElse(fail("Expected plan"))

      UpdatesCreator.queriesDeletingDate(project.resourceId, plan, List(plan.dateCreated)) shouldBe Nil
    }
  }

  private def findPlanDateCreated(projectId: projects.ResourceId, planId: plans.ResourceId)(implicit
      pcc: ProjectsConnectionConfig
  ): IO[List[plans.DateCreated]] =
    runSelect(
      SparqlQuery.of(
        "fetch agent",
        Prefixes of (prov -> "prov", schema -> "schema"),
        sparql"""|SELECT ?dateCreated
                 |FROM ${GraphClass.Project.id(projectId)} {
                 |  ${planId.asEntityId} a prov:Plan;
                 |                       schema:dateCreated ?dateCreated.
                 |}
                 |""".stripMargin
      )
    ).map(
      _.flatMap(row =>
        row.get("dateCreated").map(Instant.parse).map(plans.DateCreated.from).map(_.fold(throw _, identity))
      )
    )

  private implicit lazy val ioLogger: TestLogger[IO] = TestLogger()
}

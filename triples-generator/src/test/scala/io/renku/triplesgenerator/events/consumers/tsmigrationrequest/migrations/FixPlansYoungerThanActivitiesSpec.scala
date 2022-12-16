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
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model._
import io.renku.graph.model.entities.ProjectLens._
import io.renku.graph.model.entities._
import ActivityLens.activityStepPlan
import io.renku.graph.model.plans.DateCreated
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

import java.time.Instant

class FixPlansYoungerThanActivitiesSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "run" should {

    "fix all Plans that are associated with Activities having startTime before Plan creation" in {

      val brokenProject = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .map(movePlanDateBeforeActivity)
        .generateOne
      val validProject = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = projectsDataset, brokenProject, validProject)

      findAllDates shouldBe Set(brokenProject, validProject).flatMap(proj =>
        proj.activities.map(a => a.startTime -> activityStepPlan(proj.plans).get(a).dateCreated)
      )

      runUpdate(on = projectsDataset, FixPlansYoungerThanActivities.query).unsafeRunSync() shouldBe ()

      findAllDates shouldBe Set(
        validProject.activities
          .map(a => a.startTime -> activityStepPlan(validProject.plans).get(a).dateCreated),
        brokenProject.activities
          .map(a => a.startTime -> plans.DateCreated(a.startTime.value))
      ).flatten
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      FixPlansYoungerThanActivities[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findAllDates: Set[(activities.StartTime, plans.DateCreated)] = runSelect(
    on = projectsDataset,
    SparqlQuery.of(
      "fetch activity and plan dates",
      Prefixes of (prov -> "prov", schema -> "schema"),
      s"""|SELECT ?startTime ?dateCreated
          |WHERE {
          |  GRAPH ?projectId {
          |    ?activityId prov:startedAtTime ?startTime;
          |                prov:qualifiedAssociation/prov:hadPlan/schema:dateCreated ?dateCreated
          |  }
          |}
          |""".stripMargin
    )
  ).unsafeRunSync()
    .map(row =>
      activities.StartTime(Instant.parse(row("startTime"))) -> plans.DateCreated(Instant.parse(row("dateCreated")))
    )
    .toSet

  private def movePlanDateBeforeActivity(project: entities.RenkuProject): entities.RenkuProject = {
    val tweakedPlans = project.activities
      .map(a => a.startTime -> activityStepPlan(project.plans).get(a))
      .map { case (startTime, plan) =>
        PlanLens.planDateCreated.set(
          timestampsNotInTheFuture(butYoungerThan = startTime.value.minusSeconds(1)).generateAs(DateCreated)
        )(plan)
      }

    plansLens.set(tweakedPlans)(project)
  }
}

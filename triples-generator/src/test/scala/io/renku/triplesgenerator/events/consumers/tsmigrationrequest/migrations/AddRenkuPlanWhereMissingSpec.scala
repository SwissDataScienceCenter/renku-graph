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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations

import cats.effect.IO
import eu.timepit.refined.auto._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.entities.ProjectLens.collectCompositePlans
import io.renku.graph.model.testentities.RenkuProject.CreateCompositePlan
import io.renku.graph.model.testentities._
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, EntityType}
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class AddRenkuPlanWhereMissingSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset
    with MockFactory {

  "run" should {

    "find all prov:Plan entities that does not have renku:Plan and add it" in {

      val project1 = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.Project])
        .generateOne
      val project2 = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.Project])
        .generateOne
      val project3 = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .addCompositePlan(CreateCompositePlan(compositePlanEntities(personEntities, _)))
        .to[entities.Project]

      upload(to = projectsDataset, project1, project2, project3)

      findPlanTypes(project1.resourceId,
                    project1.plans.head.resourceId
      ) should contain theSameElementsAs entities.StepPlan.entityTypes.toList
      findPlanTypes(project2.resourceId,
                    project2.plans.head.resourceId
      ) should contain theSameElementsAs entities.StepPlan.entityTypes.toList
      findPlanTypes(project3.resourceId,
                    collectCompositePlans(project3.plans).head.resourceId
      ) should contain theSameElementsAs entities.CompositePlan.Ontology.entityTypes.toList

      delete(
        from = projectsDataset,
        Quad(GraphClass.Project.id(project2.resourceId),
             project2.plans.head.resourceId.asEntityId,
             rdf / "type",
             EntityId.of(renku / "Plan")
        )
      )
      findPlanTypes(project2.resourceId,
                    project2.plans.head.resourceId
      ) should contain theSameElementsAs entities.StepPlan.entityTypes.toList
        .filterNot(_ == EntityType.of(renku / "Plan"))

      runUpdate(on = projectsDataset, AddRenkuPlanWhereMissing.query).unsafeRunSync() shouldBe ()

      findPlanTypes(project1.resourceId,
                    project1.plans.head.resourceId
      ) should contain theSameElementsAs entities.StepPlan.entityTypes.toList
      findPlanTypes(project2.resourceId,
                    project2.plans.head.resourceId
      ) should contain theSameElementsAs entities.StepPlan.entityTypes.toList
      findPlanTypes(
        project3.resourceId,
        collectCompositePlans(project3.plans).head.resourceId
      ) should contain theSameElementsAs entities.CompositePlan.Ontology.entityTypes.toList
    }
  }

  "apply" should {
    "return an QueryBasedMigration" in {
      implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
      implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
      implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
      AddRenkuPlanWhereMissing[IO].unsafeRunSync().getClass shouldBe classOf[UpdateQueryMigration[IO]]
    }
  }

  private def findPlanTypes(projectId: projects.ResourceId, planId: plans.ResourceId): Set[EntityType] = runSelect(
    on = projectsDataset,
    SparqlQuery.of(
      "fetch plans' types",
      Prefixes of (rdf -> "rdf", prov -> "prov"),
      s"""|SELECT ?type
          |FROM <${GraphClass.Project.id(projectId)}> {
          |  <$planId> rdf:type ?type.
          |}
          |""".stripMargin
    )
  ).unsafeRunSync()
    .map(row => EntityType.of(row("type")))
    .toSet
}

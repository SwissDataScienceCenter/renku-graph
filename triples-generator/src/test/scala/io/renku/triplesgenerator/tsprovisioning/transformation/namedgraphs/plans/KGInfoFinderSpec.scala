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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.plans

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, plans}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGInfoFinderSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "findCreatedDates" should {

    "return the sole plan's dateCreated" in new TestCase {

      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val plan = project.plans.headOption.getOrElse(fail("Plan expected"))

      upload(to = projectsDataset, project)

      kgInfoFinder.findCreatedDates(project.resourceId, plan.resourceId).unsafeRunSync() shouldBe List(plan.dateCreated)
    }

    "return all plan's dateCreated" in new TestCase {

      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val plan = project.plans.headOption.getOrElse(fail("Plan expected"))

      upload(to = projectsDataset, project)

      val otherDate = timestampsNotInTheFuture(butYoungerThan = plan.dateCreated.value).generateOne
      insert(to = projectsDataset,
             Quad(GraphClass.Project.id(project.resourceId),
                  plan.resourceId.asEntityId,
                  schema / "dateCreated",
                  otherDate.asTripleObject
             )
      )

      kgInfoFinder.findCreatedDates(project.resourceId, plan.resourceId).unsafeRunSync() shouldBe
        List(plans.DateCreated(otherDate), plan.dateCreated).sorted
    }

    "return no dateCreated if there's no Plan with the given id" in new TestCase {
      kgInfoFinder
        .findCreatedDates(projectResourceIds.generateOne, planResourceIds.generateOne)
        .unsafeRunSync() shouldBe Nil
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    val kgInfoFinder = new KGInfoFinderImpl[IO](projectsDSConnectionInfo)
  }
}

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
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.timestampsNotInTheFuture
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities, plans}
import io.renku.interpreters.TestLogger
import io.renku.jsonld.syntax._
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesgenerator.TriplesGeneratorJenaSpec
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.scalatest.Succeeded
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

class KGInfoFinderSpec extends AsyncWordSpec with AsyncIOSpec with TriplesGeneratorJenaSpec with should.Matchers {

  "findCreatedDates" should {

    "return the sole plan's dateCreated" in projectsDSConfig.use { implicit pcc =>
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val plan = project.plans.headOption.getOrElse(fail("Plan expected"))

      uploadToProjects(project) >>
        kgInfoFinder.findCreatedDates(project.resourceId, plan.resourceId).asserting(_ shouldBe List(plan.dateCreated))
    }

    "return all plan's dateCreated" in projectsDSConfig.use { implicit pcc =>
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val plan = project.plans.headOption.getOrElse(fail("Plan expected"))

      for {
        _ <- uploadToProjects(project)

        otherDate = timestampsNotInTheFuture(butYoungerThan = plan.dateCreated.value).generateOne
        _ <- insert(
               Quad(GraphClass.Project.id(project.resourceId),
                    plan.resourceId.asEntityId,
                    schema / "dateCreated",
                    otherDate.asTripleObject
               )
             )

        _ <- kgInfoFinder
               .findCreatedDates(project.resourceId, plan.resourceId)
               .asserting(_ shouldBe List(plans.DateCreated(otherDate), plan.dateCreated).sorted)
      } yield Succeeded
    }

    "return no dateCreated if there's no Plan with the given id" in projectsDSConfig.use { implicit pcc =>
      kgInfoFinder
        .findCreatedDates(projectResourceIds.generateOne, planResourceIds.generateOne)
        .asserting(_ shouldBe Nil)
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger[IO]()
  private def kgInfoFinder(implicit pcc: ProjectsConnectionConfig) = {
    implicit val tr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    new KGInfoFinderImpl[IO](pcc)
  }
}

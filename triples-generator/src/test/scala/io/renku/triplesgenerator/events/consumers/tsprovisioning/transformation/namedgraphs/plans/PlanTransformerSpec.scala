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

package io.renku.triplesgenerator.events.consumers.tsprovisioning
package transformation
package namedgraphs.plans

import Generators.recoverableClientErrors
import TransformationStep.Queries
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{exceptions, timestampsNotInTheFuture}
import io.renku.graph.model._
import io.renku.graph.model.entities.ProjectLens._
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesstore.SparqlQuery
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class PlanTransformerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "createTransformationStep" should {

    "create update queries for changed plans' creation dates" in new TestCase {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()).multiple: _*)
        .generateOne
        .to[entities.RenkuProject]

      val updateQueries = collectStepPlans(project.plans) >>= givenDateUpdates(project.resourceId)

      val step = transformer.createTransformationStep

      (step run project).value shouldBe (project -> Queries.preDataQueriesOnly(updateQueries)).asRight.pure[Try]
    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val exception = recoverableClientErrors.generateOne
      findingPlanDateCreated(project.resourceId,
                             project.plans.head.resourceId,
                             returning = exception.raiseError[Try, Nothing]
      )

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(project).value

      recoverableError          shouldBe a[ProcessingRecoverableError]
      recoverableError.getMessage should startWith("Problem finding activity details in KG")
    }

    "fail with NonRecoverableFailure if calls to KG fails with an unknown exception" in new TestCase {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val exception = exceptions.generateOne
      findingPlanDateCreated(project.resourceId,
                             project.plans.head.resourceId,
                             returning = exception.raiseError[Try, Nothing]
      )

      transformer.createTransformationStep.run(project).value shouldBe
        exception.raiseError[Try, Either[ProcessingRecoverableError, (RenkuProject, Queries)]]
    }
  }

  private trait TestCase {
    private val kgInfoFinder   = mock[KGInfoFinder[Try]]
    private val updatesCreator = mock[UpdatesCreator]
    val transformer            = new PlanTransformerImpl[Try](kgInfoFinder, updatesCreator)

    def givenDateUpdates(projectId: projects.ResourceId)(plan: entities.StepPlan): List[SparqlQuery] = {
      val maybeDateCreatedInKG = timestampsNotInTheFuture.toGeneratorOf(plans.DateCreated).generateList(max = 2)
      findingPlanDateCreated(projectId, plan.resourceId, returning = maybeDateCreatedInKG.pure[Try])

      val updateQueries = sparqlQueries.generateList()
      prepareQueriesUpdatingDates(projectId, plan, maybeDateCreatedInKG, returning = updateQueries)

      updateQueries
    }

    def findingPlanDateCreated(projectId:  projects.ResourceId,
                               resourceId: plans.ResourceId,
                               returning:  Try[List[plans.DateCreated]]
    ) = (kgInfoFinder
      .findCreatedDates(_: projects.ResourceId, _: plans.ResourceId))
      .expects(projectId, resourceId)
      .returning(returning)

    private def prepareQueriesUpdatingDates(projectId: projects.ResourceId,
                                            plan:      entities.StepPlan,
                                            tsDates:   List[plans.DateCreated],
                                            returning: List[SparqlQuery]
    ) = (updatesCreator
      .queriesDeletingDate(_: projects.ResourceId, _: entities.StepPlan, _: List[plans.DateCreated]))
      .expects(projectId, plan, tsDates)
      .returning(returning)
  }
}

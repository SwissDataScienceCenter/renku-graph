/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.activities

import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.userResourceIds
import io.renku.graph.model.testentities._
import io.renku.graph.model.{activities, entities, users}
import io.renku.rdfstore.SparqlQuery
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.Queries
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.Generators.recoverableClientErrors
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.TransformationStepsCreator.TransformationRecoverableError
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ActivityTransformerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "createTransformationStep" should {

    "create update queries for changed/deleted activities authors" in new TestCase {
      val project = projectEntities(anyVisibility)
        .withActivities(activityEntities(planEntities()).many: _*)
        .generateOne
        .to[entities.ProjectWithoutParent]

      val authorUnlinkingQueries = project.activities >>= givenAuthorUnlinking

      val step = transformer.createTransformationStep

      step.run(project).value shouldBe (project -> Queries.preDataQueriesOnly(authorUnlinkingQueries)).asRight.pure[Try]
    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {
      val project = projectEntities(anyVisibility)
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .to[entities.ProjectWithoutParent]

      val exception = recoverableClientErrors.generateOne
      findingActivityAuthorFor(project.activities.head.resourceId,
                               returning = exception.raiseError[Try, Option[users.ResourceId]]
      )

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(project).value

      recoverableError            shouldBe a[TransformationRecoverableError]
      recoverableError.getMessage shouldBe "Problem finding activity details in KG"
    }

    "fail with NonRecoverableFailure if calls to KG fails with an unknown exception" in new TestCase {
      val project = projectEntities(anyVisibility)
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .to[entities.ProjectWithoutParent]

      val exception = exceptions.generateOne
      findingActivityAuthorFor(project.activities.head.resourceId,
                               returning = exception.raiseError[Try, Option[users.ResourceId]]
      )

      transformer.createTransformationStep.run(project).value shouldBe
        exception.raiseError[Try, Either[ProcessingRecoverableError, (Project, Queries)]]
    }
  }

  private trait TestCase {
    val kgInfoFinder   = mock[KGInfoFinder[Try]]
    val updatesCreator = mock[UpdatesCreator]
    val transformer    = new ActivityTransformerImpl[Try](kgInfoFinder, updatesCreator)

    def givenAuthorUnlinking(activity: entities.Activity): List[SparqlQuery] = {
      val maybeCreatorInKG = userResourceIds.generateOption
      findingActivityAuthorFor(activity.resourceId, returning = maybeCreatorInKG.pure[Try])

      val unlinkingQueries = sparqlQueries.generateList()
      prepareQueriesUnlinkingCreator(activity, maybeCreatorInKG, returning = unlinkingQueries)

      unlinkingQueries
    }

    def findingActivityAuthorFor(resourceId: activities.ResourceId, returning: Try[Option[users.ResourceId]]) =
      (kgInfoFinder
        .findActivityAuthor(_: activities.ResourceId))
        .expects(resourceId)
        .returning(returning)

    def prepareQueriesUnlinkingCreator(
        activity:      entities.Activity,
        maybeKgAuthor: Option[users.ResourceId],
        returning:     List[SparqlQuery]
    ) = (updatesCreator
      .queriesUnlinkingAuthor(_: entities.Activity, _: Option[users.ResourceId]))
      .expects(activity, maybeKgAuthor)
      .returning(returning)
  }
}

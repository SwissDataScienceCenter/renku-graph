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

package io.renku.triplesgenerator.events.consumers.tsprovisioning
package transformation
package namedgraphs.activities

import Generators.recoverableClientErrors
import TransformationStep.Queries
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators.personResourceIds
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesstore.SparqlQuery
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ActivityTransformerSpec extends AnyWordSpec with should.Matchers with MockFactory {

  "createTransformationStep" should {

    "create update queries for changed/deleted activities' authors " +
      "and associations' agents" in new TestCase {
        val project = anyRenkuProjectEntities
          .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent).multiple: _*)
          .generateOne
          .to[entities.RenkuProject]

        val authorUnlinkingQueries = project.activities >>= givenAuthorUnlinking(project.resourceId)
        val agentUnlinkingQueries  = project.activities >>= givenAgentUnlinking(project.resourceId)

        val step = transformer.createTransformationStep

        step.run(project).value shouldBe (
          project -> Queries.preDataQueriesOnly(authorUnlinkingQueries ::: agentUnlinkingQueries)
        ).asRight.pure[Try]
      }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error " +
      "- failure in the author unlinking flow" in new TestCase {
        val project = anyRenkuProjectEntities
          .withActivities(activityEntities(planEntities()))
          .generateOne
          .to[entities.RenkuProject]

        val exception = recoverableClientErrors.generateOne
        findingActivityAuthorsFor(project.resourceId,
                                  project.activities.head.resourceId,
                                  returning = exception.raiseError[Try, Set[persons.ResourceId]]
        )

        val step = transformer.createTransformationStep

        val Success(Left(recoverableError)) = step.run(project).value

        recoverableError          shouldBe a[ProcessingRecoverableError]
        recoverableError.getMessage should startWith("Problem finding activity details in KG")
      }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error " +
      "- failure in the agent unlinking flow" in new TestCase {
        val project = anyRenkuProjectEntities
          .withActivities(activityEntities(planEntities()))
          .generateOne
          .to[entities.RenkuProject]

        project.activities >>= givenAuthorUnlinking(project.resourceId)

        val exception = recoverableClientErrors.generateOne
        findingAssociationPersonAgentsFor(project.resourceId,
                                          project.activities.head.resourceId,
                                          returning = exception.raiseError[Try, Set[persons.ResourceId]]
        )

        val step = transformer.createTransformationStep

        val Success(Left(recoverableError)) = step.run(project).value

        recoverableError          shouldBe a[ProcessingRecoverableError]
        recoverableError.getMessage should startWith("Problem finding activity details in KG")
      }

    "fail with NonRecoverableFailure if calls to KG fails with an unknown exception " +
      "- failure in the author unlinking flow" in new TestCase {
        val project = anyRenkuProjectEntities
          .withActivities(activityEntities(planEntities()))
          .generateOne
          .to[entities.RenkuProject]

        val exception = exceptions.generateOne
        findingActivityAuthorsFor(project.resourceId,
                                  project.activities.head.resourceId,
                                  returning = exception.raiseError[Try, Set[persons.ResourceId]]
        )

        transformer.createTransformationStep.run(project).value shouldBe
          exception.raiseError[Try, Either[ProcessingRecoverableError, (RenkuProject, Queries)]]
      }

    "fail with NonRecoverableFailure if calls to KG fails with an unknown exception " +
      "- failure in the agent unlinking flow" in new TestCase {
        val project = anyRenkuProjectEntities
          .withActivities(activityEntities(planEntities()))
          .generateOne
          .to[entities.RenkuProject]

        project.activities >>= givenAuthorUnlinking(project.resourceId)

        val exception = exceptions.generateOne
        findingAssociationPersonAgentsFor(project.resourceId,
                                          project.activities.head.resourceId,
                                          returning = exception.raiseError[Try, Set[persons.ResourceId]]
        )

        transformer.createTransformationStep.run(project).value shouldBe
          exception.raiseError[Try, Either[ProcessingRecoverableError, (RenkuProject, Queries)]]
      }
  }

  private trait TestCase {
    val kgInfoFinder   = mock[KGInfoFinder[Try]]
    val updatesCreator = mock[UpdatesCreator]
    val transformer    = new ActivityTransformerImpl[Try](kgInfoFinder, updatesCreator)

    def givenAuthorUnlinking(projectId: projects.ResourceId)(activity: entities.Activity): List[SparqlQuery] = {
      val maybeCreatorsInKG = personResourceIds.generateSet()
      findingActivityAuthorsFor(projectId, activity.resourceId, returning = maybeCreatorsInKG.pure[Try])

      val unlinkingQueries = sparqlQueries.generateList()
      prepareQueriesUnlinkingAuthors(projectId, activity, maybeCreatorsInKG, returning = unlinkingQueries)

      unlinkingQueries
    }

    def givenAgentUnlinking(projectId: projects.ResourceId)(activity: entities.Activity): List[SparqlQuery] = {
      val maybePersonAgentsInKG = personResourceIds.generateSet()
      findingAssociationPersonAgentsFor(projectId, activity.resourceId, returning = maybePersonAgentsInKG.pure[Try])

      val unlinkingQueries = sparqlQueries.generateList()
      prepareQueriesUnlinkingAgents(projectId, activity, maybePersonAgentsInKG, returning = unlinkingQueries)

      unlinkingQueries
    }

    def findingActivityAuthorsFor(projectId:  projects.ResourceId,
                                  resourceId: activities.ResourceId,
                                  returning:  Try[Set[persons.ResourceId]]
    ) = (kgInfoFinder
      .findActivityAuthors(_: projects.ResourceId, _: activities.ResourceId))
      .expects(projectId, resourceId)
      .returning(returning)

    def findingAssociationPersonAgentsFor(projectId:  projects.ResourceId,
                                          resourceId: activities.ResourceId,
                                          returning:  Try[Set[persons.ResourceId]]
    ) = (kgInfoFinder
      .findAssociationPersonAgents(_: projects.ResourceId, _: activities.ResourceId))
      .expects(projectId, resourceId)
      .returning(returning)

    def prepareQueriesUnlinkingAuthors(projectId: projects.ResourceId,
                                       activity:  entities.Activity,
                                       kgAuthors: Set[persons.ResourceId],
                                       returning: List[SparqlQuery]
    ) = (updatesCreator
      .queriesUnlinkingAuthors(_: projects.ResourceId, _: entities.Activity, _: Set[persons.ResourceId]))
      .expects(projectId, activity, kgAuthors)
      .returning(returning)

    def prepareQueriesUnlinkingAgents(projectId: projects.ResourceId,
                                      activity:  entities.Activity,
                                      kgAgents:  Set[persons.ResourceId],
                                      returning: List[SparqlQuery]
    ) = (updatesCreator
      .queriesUnlinkingAgents(_: projects.ResourceId, _: entities.Activity, _: Set[persons.ResourceId]))
      .expects(projectId, activity, kgAgents)
      .returning(returning)
  }
}

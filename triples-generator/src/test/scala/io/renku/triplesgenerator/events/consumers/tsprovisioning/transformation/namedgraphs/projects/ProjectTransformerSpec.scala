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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation
package namedgraphs.projects

import Generators._
import cats.data.{NonEmptyList => Nel}
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.testentities.generators.EntitiesGenerators
import io.renku.graph.model.{GraphModelGenerators, entities}
import io.renku.triplesgenerator.events.consumers.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.Queries
import io.renku.triplesgenerator.events.consumers.tsprovisioning.TransformationStep.Queries.{postDataQueriesOnly, preDataQueriesOnly}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.{EitherValues, TryValues}

import scala.util.Try

class ProjectTransformerSpec
    extends AnyWordSpec
    with MockFactory
    with should.Matchers
    with EitherValues
    with TryValues
    with EntitiesGenerators {

  "createTransformationStep" should {

    "create preDataUploadQueries when project is in KG" in new TestCase {

      val kgProjectData = projectMutableDataGen.generateOne
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(kgProjectData.some.pure[Try])

      val step1Result @ (step1UpdatedProject, step1Queries) = generateProjAndQueries
      (dateCreatedUpdater
        .updateDateCreated(_: ProjectMutableData))
        .expects(kgProjectData)
        .returning(transformation(in = project -> Queries.empty, out = step1Result))

      val step2Result @ (step2UpdatedProject, step2Queries) = generateProjAndQueries
      (dateModifiedUpdater
        .updateDateModified(_: ProjectMutableData))
        .expects(kgProjectData)
        .returning(transformation(in = step1UpdatedProject -> step1Queries, out = step2Result))

      val otherQueries = sparqlQueries.generateList()
      val postQueries  = sparqlQueries.generateList()
      (updatesCreator.prepareUpdates _)
        .expects(step2UpdatedProject, kgProjectData)
        .returning(otherQueries)

      (updatesCreator.postUpdates _)
        .expects(step2UpdatedProject)
        .returning(postQueries)

      val step = transformer.createTransformationStep

      step.name.value shouldBe "Project Details Updates"
      val Right(updateResult) = (step run project).value.success.value
      updateResult shouldBe (
        step2UpdatedProject,
        List(step2Queries, preDataQueriesOnly(otherQueries), postDataQueriesOnly(postQueries)).combineAll
      )
    }

    "do nothing if no project found in KG" in new TestCase {

      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(None.pure[Try])

      val step = transformer.createTransformationStep

      val Right(updateResult) = step.run(project).value.success.value
      updateResult shouldBe (project, Queries.empty)
    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {

      val exception = recoverableClientErrors.generateOne
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(exception.raiseError[Try, Option[ProjectMutableData]])

      val step = transformer.createTransformationStep

      val recoverableError = step.run(project).value.success.value.left.value

      recoverableError          shouldBe a[ProcessingRecoverableError]
      recoverableError.getMessage should startWith("Problem finding project details in KG")
    }

    "fail with NonRecoverableFailure if finding calls to KG fails with an unknown exception" in new TestCase {

      val exception = exceptions.generateOne
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(exception.raiseError[Try, Option[ProjectMutableData]])

      val step = transformer.createTransformationStep

      step.run(project).value.failure.exception shouldBe exception
    }
  }

  private trait TestCase {

    def transformation(in:  (entities.Project, Queries),
                       out: (entities.Project, Queries)
    ): ((entities.Project, Queries)) => (entities.Project, Queries) = {
      case `in` => out
      case _    => fail("Project or Queries different than expected")
    }

    val kgProjectFinder     = mock[KGProjectFinder[Try]]
    val updatesCreator      = mock[UpdatesCreator]
    val dateCreatedUpdater  = mock[DateCreatedUpdater]
    val dateModifiedUpdater = mock[DateModifiedUpdater]
    val transformer =
      new ProjectTransformerImpl[Try](kgProjectFinder, updatesCreator, dateCreatedUpdater, dateModifiedUpdater)

    val project = renkuProjectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
  }

  private lazy val projectMutableDataGen: Gen[ProjectMutableData] = for {
    name              <- projectNames
    dateCreated       <- projectCreatedDates()
    maybeDateModified <- projectModifiedDates(dateCreated.value).toGeneratorOfOptions
    maybeParentId     <- projectResourceIds.toGeneratorOfOptions
    visibility        <- projectVisibilities
    maybeDesc         <- projectDescriptions.toGeneratorOfOptions
    keywords          <- projectKeywords.toGeneratorOfSet(min = 0)
    maybeAgent        <- GraphModelGenerators.cliVersions.toGeneratorOfOptions
    maybeCreatorId    <- personResourceIds.toGeneratorOfOptions
    images            <- imageUris.toGeneratorOfList()
  } yield ProjectMutableData(name,
                             Nel.of(dateCreated),
                             maybeDateModified.toList,
                             maybeParentId,
                             visibility,
                             maybeDesc,
                             keywords,
                             maybeAgent,
                             maybeCreatorId,
                             images
  )

  private def generateProjAndQueries =
    projectEntities(anyVisibility).generateOne.to[entities.Project] -> queriesGen.generateOne
}

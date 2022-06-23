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

package io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.projects

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.tsprovisioning.TransformationStep.Queries
import io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.Generators.recoverableClientErrors
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class ProjectTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createTransformationStep" should {

    "create preDataUploadQueries when project is in KG" in new TestCase {
      val kgProjectInfo = projectMutableDataGen.generateOne

      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(kgProjectInfo.some.pure[Try])

      val queries = sparqlQueries.generateList()

      (updatesCreator.prepareUpdates _)
        .expects(project, kgProjectInfo)
        .returning(queries)

      val step = transformer.createTransformationStep

      step.name.value shouldBe "Project Details Updates"
      val Success(Right(updateResult)) = (step run project).value
      updateResult shouldBe (project, Queries(queries, Nil))
    }

    "do nothing if no project found in KG" in new TestCase {
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(None.pure[Try])

      val step = transformer.createTransformationStep

      val Success(Right(updateResult)) = step.run(project).value
      updateResult shouldBe (project, Queries.empty)
    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {
      val exception = recoverableClientErrors.generateOne
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(exception.raiseError[Try, Option[ProjectMutableData]])

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(project).value

      recoverableError          shouldBe a[ProcessingRecoverableError]
      recoverableError.getMessage should startWith("Problem finding project details in KG")
    }

    "fail with NonRecoverableFailure if finding calls to KG fails with an unknown exception" in new TestCase {
      val exception = exceptions.generateOne
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(exception.raiseError[Try, Option[ProjectMutableData]])

      val step = transformer.createTransformationStep

      step.run(project).value shouldBe exception
        .raiseError[Try, Either[ProcessingRecoverableError, (RenkuProject, Queries)]]
    }
  }

  private trait TestCase {

    val kgProjectFinder = mock[KGProjectFinder[Try]]
    val updatesCreator  = mock[UpdatesCreator]
    val transformer     = new ProjectTransformerImpl[Try](kgProjectFinder, updatesCreator)

    val project = renkuProjectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
  }

  private lazy val projectMutableDataGen: Gen[ProjectMutableData] = for {
    name           <- projectNames
    dateCreated    <- projectCreatedDates()
    maybeParentId  <- projectResourceIds.toGeneratorOfOptions
    visibility     <- projectVisibilities
    maybeDesc      <- projectDescriptions.toGeneratorOfOptions
    keywords       <- projectKeywords.toGeneratorOfSet(minElements = 0)
    maybeAgent     <- cliVersions.toGeneratorOfOptions
    maybeCreatorId <- personResourceIds.toGeneratorOfOptions
  } yield ProjectMutableData(name,
                             dateCreated,
                             maybeParentId,
                             visibility,
                             maybeDesc,
                             keywords,
                             maybeAgent,
                             maybeCreatorId
  )
}
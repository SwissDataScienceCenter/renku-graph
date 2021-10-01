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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.{clientExceptions, connectivityExceptions, sparqlQueries, unexpectedResponseExceptions}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.exceptions
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.entities
import ch.datascience.graph.model.testentities._
import ch.datascience.http.client.RestClientError
import ch.datascience.http.client.RestClientError.UnauthorizedException
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep.ResultData
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.TransformationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.KGProjectFinder.KGProjectInfo
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class ProjectTransformerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "createTransformationStep" should {

    "create update queries when project is in KG" in new TestCase {
      val kgProjectInfo = (projectNames.generateOne, projectResourceIds.generateOption, projectVisibilities.generateOne)

      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(kgProjectInfo.some.pure[Try])

      val updates = sparqlQueries.generateList()

      (updatesCreator.prepareUpdates _)
        .expects(project, kgProjectInfo)
        .returning(updates)

      val step = transformer.createTransformationStep

      step.name.value shouldBe "Project Details Updates"
      val Success(Right(updateResult)) = (step run project).value
      updateResult shouldBe ResultData(project, updates)
    }

    "do nothing if no project found in KG" in new TestCase {
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(None.pure[Try])

      val step = transformer.createTransformationStep

      val Success(Right(updateResult)) = step.run(project).value
      updateResult shouldBe ResultData(project, Nil)
    }

    "return the ProcessingRecoverableFailure if calls to KG fails with a network or HTTP error" in new TestCase {
      val exception = recoverableClientErrors.generateOne
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(exception.raiseError[Try, Option[KGProjectInfo]])

      val step = transformer.createTransformationStep

      val Success(Left(recoverableError)) = step.run(project).value

      recoverableError            shouldBe a[TransformationRecoverableError]
      recoverableError.getMessage shouldBe "Problem finding project details in KG"
    }

    "fail with NonRecoverableFailure if finding calls to KG fails with an unknown exception" in new TestCase {
      val exception = exceptions.generateOne
      (kgProjectFinder.find _)
        .expects(project.resourceId)
        .returning(exception.raiseError[Try, Option[KGProjectInfo]])

      val step = transformer.createTransformationStep

      val Failure(nonRecoverableError) = step.run(project).value
      nonRecoverableError shouldBe exception
    }
  }

  private trait TestCase {

    val kgProjectFinder = mock[KGProjectFinder[Try]]
    val updatesCreator  = mock[UpdatesCreator]
    val transformer     = new ProjectTransformerImpl[Try](kgProjectFinder, updatesCreator)

    val project = projectEntitiesWithDatasetsAndActivities.generateOne.to[entities.Project]
  }

  private lazy val recoverableClientErrors: Gen[RestClientError] =
    Gen.oneOf(clientExceptions, connectivityExceptions, unexpectedResponseExceptions, Gen.const(UnauthorizedException))
}

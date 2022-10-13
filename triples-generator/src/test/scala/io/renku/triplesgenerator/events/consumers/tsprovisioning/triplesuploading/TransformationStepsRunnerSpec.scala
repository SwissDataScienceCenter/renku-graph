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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package triplesuploading

import TransformationStep.{ProjectWithQueries, Queries}
import TriplesUploadResult._
import cats.data.EitherT
import cats.data.EitherT.rightT
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.personNames
import io.renku.graph.model.testentities._
import io.renku.graph.model.{TSVersion, entities}
import io.renku.triplesgenerator.generators.ErrorGenerators.{logWorthyRecoverableErrors, processingRecoverableErrors}
import org.scalacheck.Gen
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import transformation.Generators._

import scala.util.{Success, Try}

class TransformationStepsRunnerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    s"return $DeliverySuccess if updates and triples uploading is successful" in new TestCase {
      val originalProject     = anyProjectEntities.generateOne.to[entities.Project]
      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]
      val step1Project        = anyProjectEntities.generateOne.to[entities.Project]
      val step1Queries        = queriesGen.generateOne
      step1Transformation
        .expects(originalProject)
        .returning(rightT[Try, ProcessingRecoverableError]((step1Project, step1Queries)))

      val step2Project        = anyProjectEntities.generateOne.to[entities.Project]
      val step2Queries        = queriesGen.generateOne
      val step2Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]
      step2Transformation
        .expects(step1Project)
        .returning(rightT[Try, ProcessingRecoverableError]((step2Project, step2Queries)))

      inSequence {
        step1Queries.preDataUploadQueries.foreach { query =>
          (resultsUploader.execute _).expects(query).returning(rightT(()))
        }
        step2Queries.preDataUploadQueries.foreach { query =>
          (resultsUploader.execute _).expects(query).returning(rightT(()))
        }

        (resultsUploader.upload _)
          .expects(step2Project)
          .returning(rightT(()))

        step1Queries.postDataUploadQueries.foreach { query =>
          (resultsUploader.execute _).expects(query).returning(rightT(()))
        }
        step2Queries.postDataUploadQueries.foreach { query =>
          (resultsUploader.execute _).expects(query).returning(rightT(()))
        }
      }

      stepsRunner.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation),
             TransformationStep(nonBlankStrings().generateOne, step2Transformation)
        ),
        originalProject
      )(tsVersion) shouldBe DeliverySuccess.pure[Try]
    }

    s"return $RecoverableFailure if running transformation step fails with a LogWorthyRecoverableError" in new TestCase {
      val originalProject     = anyProjectEntities.generateOne.to[entities.Project]
      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val recoverableError = logWorthyRecoverableErrors.generateOne
      step1Transformation
        .expects(originalProject)
        .returning(EitherT.leftT[Try, (entities.Project, Queries)](recoverableError))

      stepsRunner.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)), originalProject)(
        tsVersion
      ) shouldBe RecoverableFailure(recoverableError).pure[Try]
    }

    s"return $NonRecoverableFailure if a transformation step fails to run" in new TestCase {
      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Name           = nonBlankStrings().generateOne
      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val exception = exceptions.generateOne
      step1Transformation
        .expects(originalProject)
        .returning(EitherT(exception.raiseError[Try, Either[ProcessingRecoverableError, (entities.Project, Queries)]]))

      stepsRunner.run(List(TransformationStep(step1Name, step1Transformation)), originalProject)(
        tsVersion
      ) shouldBe NonRecoverableFailure(s"Transformation of ${originalProject.path} failed: $exception", exception)
        .pure[Try]
    }

    s"return $RecoverableFailure if executing the transformation step preDataUploadQueries fails with a RecoverableFailure" in new TestCase {
      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val step1Project = anyProjectEntities.generateOne.to[entities.Project]
      val step1Queries = queriesGen.generateOne.copy(preDataUploadQueries = sparqlQueries.generateNonEmptyList().toList)
      step1Transformation
        .expects(originalProject)
        .returning(rightT[Try, ProcessingRecoverableError]((step1Project, step1Queries)))

      val recoverableError = logWorthyRecoverableErrors.generateOne
      (resultsUploader.execute _)
        .expects(step1Queries.preDataUploadQueries.head)
        .returning(EitherT.leftT(recoverableError))

      stepsRunner.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
        originalProject
      )(tsVersion) shouldBe RecoverableFailure(recoverableError).pure[Try]
    }

    s"return $NonRecoverableFailure if executing transformation step preDataUploadQueries fails with a NonRecoverableFailure" in new TestCase {
      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val step1Project = anyProjectEntities.generateOne.to[entities.Project]
      val step1Queries = Queries(sparqlQueries.generateNonEmptyList().toList, Nil)
      step1Transformation
        .expects(originalProject)
        .returning(rightT[Try, ProcessingRecoverableError]((step1Project, step1Queries)))

      val nonRecoverableError = exceptions.generateOne
      (resultsUploader.execute _)
        .expects(step1Queries.preDataUploadQueries.head)
        .returning(EitherT(nonRecoverableError.raiseError[Try, Either[ProcessingRecoverableError, Unit]]))

      stepsRunner.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)), originalProject)(
        tsVersion
      ) shouldBe NonRecoverableFailure(s"Transformation of ${originalProject.path} failed: $nonRecoverableError",
                                       nonRecoverableError
      ).pure[Try]
    }

    "return NonRecoverableFailure if triples encoding fails with such a failure" in new TestCase {
      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      // preparing a project for which json-ld flattening fails
      val Some(person) = personEntities(withGitLabId).generateOne.toMaybe[entities.Person.WithGitLabId]
      val step1Project = renkuProjectEntities(anyVisibility).generateOne
        .to[entities.RenkuProject.WithoutParent]
        .copy(
          maybeCreator = person.some,
          members = Set(person.copy(name = personNames.generateOne))
        )
      step1Transformation
        .expects(originalProject)
        .returning(rightT[Try, ProcessingRecoverableError]((step1Project, Queries.empty)))

      val Success(result) = stepsRunner.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
        originalProject
      )(tsVersion)

      result shouldBe a[NonRecoverableFailure]
    }

    s"return $RecoverableFailure if triples uploading failed with RecoverableFailure" in new TestCase {

      val project = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      step1Transformation
        .expects(project)
        .returning(rightT[Try, ProcessingRecoverableError]((project, Queries.empty)))

      val failure = processingRecoverableErrors.generateOne
      (resultsUploader.upload _)
        .expects(project)
        .returning(EitherT.leftT(failure))

      stepsRunner.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)), project)(
        tsVersion
      ) shouldBe RecoverableFailure(failure).pure[Try]
    }

    s"return $NonRecoverableFailure if triples uploading fails with NonRecoverableFailure" in new TestCase {

      val project = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      step1Transformation
        .expects(project)
        .returning(rightT[Try, ProcessingRecoverableError]((project, Queries.empty)))

      val failure = exceptions.generateOne
      (resultsUploader.upload _)
        .expects(project)
        .returning(EitherT(failure.raiseError[Try, Either[ProcessingRecoverableError, Unit]]))

      stepsRunner.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)), project)(
        tsVersion
      ) shouldBe NonRecoverableFailure(s"Transformation of ${project.path} failed: $failure", failure).pure[Try]
    }

    s"return $RecoverableFailure if executing postDataUploadQueries fails with RecoverableFailure" in new TestCase {

      val project = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val step1Queries = Queries(Nil, sparqlQueries.generateNonEmptyList().toList)
      step1Transformation
        .expects(project)
        .returning(rightT[Try, ProcessingRecoverableError]((project, step1Queries)))

      (resultsUploader.upload _)
        .expects(project)
        .returning(rightT(()))

      val recoverableError = processingRecoverableErrors.generateOne
      (resultsUploader.execute _)
        .expects(step1Queries.postDataUploadQueries.head)
        .returning(EitherT.leftT(recoverableError))

      stepsRunner.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)), project)(
        tsVersion
      ) shouldBe RecoverableFailure(recoverableError).pure[Try]
    }

    s"return $NonRecoverableFailure if executing postDataUploadQueries fails with NonRecoverableFailure" in new TestCase {

      val project = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val step1Queries = Queries(Nil, sparqlQueries.generateNonEmptyList().toList)
      step1Transformation
        .expects(project)
        .returning(rightT[Try, ProcessingRecoverableError]((project, step1Queries)))

      (resultsUploader.upload _)
        .expects(project)
        .returning(rightT(()))

      val nonRecoverableError = exceptions.generateOne
      (resultsUploader.execute _)
        .expects(step1Queries.postDataUploadQueries.head)
        .returning(EitherT(nonRecoverableError.raiseError[Try, Either[ProcessingRecoverableError, Unit]]))

      stepsRunner.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)), project)(
        tsVersion
      ) shouldBe NonRecoverableFailure(s"Transformation of ${project.path} failed: $nonRecoverableError",
                                       nonRecoverableError
      ).pure[Try]
    }
  }

  private trait TestCase {
    val tsVersion              = Gen.oneOf(TSVersion.DefaultGraph, TSVersion.NamedGraphs).generateOne
    val resultsUploader        = mock[TransformationResultsUploader[Try]]
    val resultsUploaderLocator = mock[TransformationResultsUploader.Locator[Try]]
    (resultsUploaderLocator.apply _).expects(tsVersion).returning(resultsUploader)
    val stepsRunner = new TransformationStepsRunnerImpl[Try](resultsUploaderLocator)
  }
}

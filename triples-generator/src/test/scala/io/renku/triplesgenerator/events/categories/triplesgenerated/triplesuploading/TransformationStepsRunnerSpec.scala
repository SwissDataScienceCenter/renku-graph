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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading

import cats.data.EitherT
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators.userNames
import io.renku.graph.model.entities
import io.renku.graph.model.testentities._
import io.renku.jsonld.syntax._
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ProjectWithQueries, Queries}
import io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.Generators._
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult._
import io.renku.triplesgenerator.generators.ErrorGenerators.{logWorthyRecoverableErrors, processingRecoverableErrors}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

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
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((step1Project, step1Queries)))

      val step2Project        = anyProjectEntities.generateOne.to[entities.Project]
      val step2Queries        = queriesGen.generateOne
      val step2Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]
      step2Transformation
        .expects(step1Project)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((step2Project, step2Queries)))

      inSequence {
        step1Queries.preDataUploadQueries.foreach { query =>
          (updatesUploader.send _).expects(query).returning(EitherT.rightT(()))
        }
        step2Queries.preDataUploadQueries.foreach { query =>
          (updatesUploader.send _).expects(query).returning(EitherT.rightT(()))
        }

        (triplesUploader.uploadTriples _)
          .expects(step2Project.asJsonLD.flatten.fold(fail(_), identity))
          .returning(EitherT.rightT(()))

        step1Queries.postDataUploadQueries.foreach { query =>
          (updatesUploader.send _).expects(query).returning(EitherT.rightT(()))
        }
        step2Queries.postDataUploadQueries.foreach { query =>
          (updatesUploader.send _).expects(query).returning(EitherT.rightT(()))
        }
      }

      uploader.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation),
             TransformationStep(nonBlankStrings().generateOne, step2Transformation)
        ),
        originalProject
      ) shouldBe DeliverySuccess.pure[Try]
    }

    s"return $RecoverableFailure if running transformation step fails with a LogWorthyRecoverableError" in new TestCase {
      val originalProject     = anyProjectEntities.generateOne.to[entities.Project]
      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val recoverableError = logWorthyRecoverableErrors.generateOne
      step1Transformation
        .expects(originalProject)
        .returning(EitherT.leftT[Try, (entities.Project, Queries)](recoverableError))

      uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                   originalProject
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

      uploader.run(List(TransformationStep(step1Name, step1Transformation)),
                   originalProject
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
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((step1Project, step1Queries)))

      val recoverableError = logWorthyRecoverableErrors.generateOne
      (updatesUploader.send _)
        .expects(step1Queries.preDataUploadQueries.head)
        .returning(EitherT.leftT(recoverableError))

      uploader.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
        originalProject
      ) shouldBe RecoverableFailure(recoverableError).pure[Try]
    }

    s"return $NonRecoverableFailure if executing transformation step preDataUploadQueries fails with a NonRecoverableFailure" in new TestCase {
      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val step1Project = anyProjectEntities.generateOne.to[entities.Project]
      val step1Queries = Queries(sparqlQueries.generateNonEmptyList().toList, Nil)
      step1Transformation
        .expects(originalProject)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((step1Project, step1Queries)))

      val nonRecoverableError = exceptions.generateOne
      (updatesUploader.send _)
        .expects(step1Queries.preDataUploadQueries.head)
        .returning(EitherT(nonRecoverableError.raiseError[Try, Either[ProcessingRecoverableError, Unit]]))

      uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                   originalProject
      ) shouldBe NonRecoverableFailure(s"Transformation of ${originalProject.path} failed: $nonRecoverableError",
                                       nonRecoverableError
      ).pure[Try]
    }

    "return NonRecoverableFailure if triples encoding failed with such failure" in new TestCase {
      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      // preparing a project for which json-ld flattening fails
      val Some(person) = personEntities(withGitLabId).generateOne.toMaybe[entities.Person.WithGitLabId]
      val step1Project = renkuProjectEntities(anyVisibility).generateOne
        .to[entities.RenkuProject.WithoutParent]
        .copy(
          maybeCreator = person.some,
          members = Set(person.copy(name = userNames.generateOne))
        )
      step1Transformation
        .expects(originalProject)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((step1Project, Queries.empty)))

      val Success(result) = uploader.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
        originalProject
      )

      result shouldBe a[NonRecoverableFailure]
    }

    s"return $RecoverableFailure if triples uploading failed with RecoverableFailure" in new TestCase {

      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      step1Transformation
        .expects(originalProject)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((originalProject, Queries.empty)))

      val failure = processingRecoverableErrors.generateOne

      (triplesUploader.uploadTriples _)
        .expects(*)
        .returning(EitherT.leftT(failure))

      uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                   originalProject
      ) shouldBe RecoverableFailure(failure).pure[Try]
    }

    s"return $NonRecoverableFailure if triples uploading failed with NonRecoverableFailure" in new TestCase {

      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      step1Transformation
        .expects(originalProject)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((originalProject, Queries.empty)))

      val failure = exceptions.generateOne

      (triplesUploader.uploadTriples _)
        .expects(*)
        .returning(EitherT(failure.raiseError[Try, Either[ProcessingRecoverableError, Unit]]))

      uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                   originalProject
      ) shouldBe NonRecoverableFailure(s"Transformation of ${originalProject.path} failed: $failure", failure).pure[Try]
    }

    s"return $RecoverableFailure if executing postDataUploadQueries failed with RecoverableFailure" in new TestCase {

      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val step1Queries = Queries(Nil, sparqlQueries.generateNonEmptyList().toList)
      step1Transformation
        .expects(originalProject)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((originalProject, step1Queries)))

      (triplesUploader.uploadTriples _)
        .expects(*)
        .returning(EitherT.rightT(()))

      val recoverableError = processingRecoverableErrors.generateOne
      (updatesUploader.send _)
        .expects(step1Queries.postDataUploadQueries.head)
        .returning(EitherT.leftT(recoverableError))

      uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                   originalProject
      ) shouldBe RecoverableFailure(recoverableError).pure[Try]
    }

    s"return $NonRecoverableFailure if executing postDataUploadQueries failed with NonRecoverableFailure" in new TestCase {

      val originalProject = anyProjectEntities.generateOne.to[entities.Project]

      val step1Transformation = mockFunction[entities.Project, ProjectWithQueries[Try]]

      val step1Queries = Queries(Nil, sparqlQueries.generateNonEmptyList().toList)
      step1Transformation
        .expects(originalProject)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError]((originalProject, step1Queries)))

      (triplesUploader.uploadTriples _)
        .expects(*)
        .returning(EitherT.rightT(()))

      val nonRecoverableError = exceptions.generateOne
      (updatesUploader.send _)
        .expects(step1Queries.postDataUploadQueries.head)
        .returning(EitherT(nonRecoverableError.raiseError[Try, Either[ProcessingRecoverableError, Unit]]))

      uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                   originalProject
      ) shouldBe NonRecoverableFailure(s"Transformation of ${originalProject.path} failed: $nonRecoverableError",
                                       nonRecoverableError
      )
        .pure[Try]
    }
  }

  private trait TestCase {
    val triplesUploader = mock[TriplesUploader[Try]]
    val updatesUploader = mock[UpdatesUploader[Try]]
    val uploader = new TransformationStepsRunnerImpl[Try](triplesUploader, updatesUploader, renkuBaseUrl, gitLabUrl)
  }
}

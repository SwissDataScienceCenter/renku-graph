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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading

import cats.data.EitherT
import cats.syntax.all._
import ch.datascience.generators.CommonGraphGenerators.sparqlQueries
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ResultData, TransformationStepResult}
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TriplesGeneratedGenerators._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplesuploading.TriplesUploadResult._
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.{ProjectMetadata, TransformationStep}
import eu.timepit.refined.auto._
import io.renku.jsonld.syntax._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TransformationStepsRunnerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "run" should {

    s"return $DeliverySuccess if updates and triples uploading is successful" in new TestCase {
      val originalMetadata = projectMetadatas.generateOne

      val step1Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]

      val step1Metadata = projectMetadatas.generateOne
      val step1Queries  = sparqlQueries.generateList()
      step1Transformation
        .expects(originalMetadata)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](ResultData(step1Metadata, step1Queries)))

      val step2Metadata       = projectMetadatas.generateOne
      val step2Queries        = sparqlQueries.generateList()
      val step2Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]
      step2Transformation
        .expects(step1Metadata)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](ResultData(step2Metadata, step2Queries)))

      inSequence {
        step1Queries.foreach { query =>
          (updatesUploader.send _).expects(query).returning(DeliverySuccess.pure[Try])
        }
        step2Queries.foreach { query =>
          (updatesUploader.send _).expects(query).returning(DeliverySuccess.pure[Try])
        }

        (triplesUploader.upload _)
          .expects(step2Metadata.asJsonLD)
          .returning(DeliverySuccess.pure[Try])
      }

      uploader.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation),
             TransformationStep(nonBlankStrings().generateOne, step2Transformation)
        ),
        originalMetadata
      ) shouldBe DeliverySuccess
        .pure[Try]
    }

    s"return $RecoverableFailure if running transformation step fails with a TransformationRecoverableError" in new TestCase {
      val originalMetadata    = projectMetadatas.generateOne
      val step1Name           = nonBlankStrings().generateOne
      val step1Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]

      val recoverableError = transformationRecoverableErrors.generateOne
      step1Transformation
        .expects(originalMetadata)
        .returning(EitherT.leftT[Try, ResultData](recoverableError))

      uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                   originalMetadata
      ) shouldBe RecoverableFailure(recoverableError.getMessage).pure[Try]
    }

    s"return $InvalidUpdatesFailure if a transformation step fails to run" in new TestCase {
      val originalMetadata = projectMetadatas.generateOne

      val step1Name           = nonBlankStrings().generateOne
      val step1Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]

      val exception = exceptions.generateOne
      step1Transformation
        .expects(originalMetadata)
        .returning(EitherT.right[ProcessingRecoverableError](exception.raiseError[Try, ResultData]))

      uploader.run(List(TransformationStep(step1Name, step1Transformation)),
                   originalMetadata
      ) shouldBe InvalidUpdatesFailure(
        s"$step1Name transformation step failed: $exception"
      ).pure[Try]
    }

    s"return $RecoverableFailure if executing transformation step queries fails with a RecoverableFailure" in new TestCase {
      val originalMetadata = projectMetadatas.generateOne

      val step1Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]
      val step2Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]

      val step1Metadata = projectMetadatas.generateOne
      val step1Queries  = sparqlQueries.generateFixedSizeList(1)
      step1Transformation
        .expects(originalMetadata)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](ResultData(step1Metadata, step1Queries)))

      val recoverableError = RecoverableFailure(nonEmptyStrings().generateOne)
      step1Queries.foreach { query =>
        (updatesUploader.send _).expects(query).returning(recoverableError.pure[Try])
      }

      uploader.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation),
             TransformationStep(nonBlankStrings().generateOne, step2Transformation)
        ),
        originalMetadata
      ) shouldBe recoverableError.pure[Try]
    }

    s"return $InvalidUpdatesFailure if executing transformation step queries fails with a InvalidUpdatesFailure" in new TestCase {
      val originalMetadata = projectMetadatas.generateOne

      val step1Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]
      val step2Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]

      val step1Metadata = projectMetadatas.generateOne
      val step1Queries  = sparqlQueries.generateFixedSizeList(1)
      step1Transformation
        .expects(originalMetadata)
        .returning(EitherT.rightT[Try, ProcessingRecoverableError](ResultData(step1Metadata, step1Queries)))

      val nonRecoverableError = InvalidUpdatesFailure(nonEmptyStrings().generateOne)
      step1Queries.foreach { query =>
        (updatesUploader.send _).expects(query).returning(nonRecoverableError.pure[Try])
      }

      uploader.run(
        List(TransformationStep(nonBlankStrings().generateOne, step1Transformation),
             TransformationStep(nonBlankStrings().generateOne, step2Transformation)
        ),
        originalMetadata
      ) shouldBe nonRecoverableError.pure[Try]
    }

    Set(RecoverableFailure(nonEmptyStrings().generateOne),
        InvalidTriplesFailure(nonEmptyStrings().generateOne)
    ) foreach { failure =>
      s"return ${failure.getClass.getSimpleName} if triples uploading failed with such failure" in new TestCase {
        val originalMetadata = projectMetadatas.generateOne

        val step1Transformation = mockFunction[ProjectMetadata, TransformationStepResult[Try]]

        val step1Metadata = projectMetadatas.generateOne
        val step1Queries  = sparqlQueries.generateList()
        step1Transformation
          .expects(originalMetadata)
          .returning(EitherT.rightT[Try, ProcessingRecoverableError](ResultData(step1Metadata, step1Queries)))

        inSequence {
          step1Queries.foreach { query =>
            (updatesUploader.send _).expects(query).returning(DeliverySuccess.pure[Try])
          }

          (triplesUploader.upload _)
            .expects(originalMetadata.asJsonLD)
            .returning(failure.pure[Try])
        }

        uploader.run(List(TransformationStep(nonBlankStrings().generateOne, step1Transformation)),
                     originalMetadata
        ) shouldBe failure.pure[Try]
      }
    }
  }

  private trait TestCase {
    val triplesUploader = mock[TriplesUploader[Try]]
    val updatesUploader = mock[UpdatesUploader[Try]]
    val uploader        = new TransformationStepsRunnerImpl[Try](triplesUploader, updatesUploader)
  }
}

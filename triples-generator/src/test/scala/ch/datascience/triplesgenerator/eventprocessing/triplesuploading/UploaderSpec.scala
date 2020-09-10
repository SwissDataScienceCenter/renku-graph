/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplesuploading

import cats.data.EitherT
import cats.data.EitherT.right
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.EventProcessingGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class UploaderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "upload" should {

    s"return $DeliverySuccess if triples and updates uploading is successful" in new TestCase {

      inSequence {
        (triplesUploader.upload _)
          .expects(curatedTriples.triples)
          .returning(DeliverySuccess.pure[Try])

        curatedTriples.updates.map { update =>
          update()
            .fold(throw _,
                  query =>
                    (updatesUploader.send _)
                      .expects(query)
                      .returning(DeliverySuccess.pure[Try]))
        }
      }

      uploader.upload(curatedTriples) shouldBe DeliverySuccess.pure[Try]
    }

    s"return $RecoverableFailure if triples uploading failed with such failure" in new TestCase {

      val failure = RecoverableFailure(nonEmptyStrings().generateOne)
      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(failure.pure[Try])

      uploader.upload(curatedTriples) shouldBe failure.pure[Try]
    }

    s"return $RecoverableFailure if the given updates fail on query creation with a ProcessingRecoverableError" in new TestCase {
      val recoverableError = processingRecoverableErrors.generateOne

      val updatedCuratedTriples = curatedTriples.copy(updates = curatedTriples.updates.map { update =>
        update.copy(
          queryGenerator = () => EitherT.leftT[Try, SparqlQuery](recoverableError)
        )
      })

      (triplesUploader.upload _)
        .expects(updatedCuratedTriples.triples)
        .returning(DeliverySuccess.pure[Try])

      uploader.upload(updatedCuratedTriples) shouldBe RecoverableFailure(recoverableError.getMessage).pure[Try]
    }

    s"return $InvalidUpdatesFailure if the given updates fail on query creation" in new TestCase {
      val exception = exceptions.generateOne

      val updatedCuratedTriples = curatedTriples.copy(updates = curatedTriples.updates.map { update =>
        update.copy(
          queryGenerator = () => right[ProcessingRecoverableError](exception.raiseError[Try, SparqlQuery])
        )
      })

      (triplesUploader.upload _)
        .expects(updatedCuratedTriples.triples)
        .returning(DeliverySuccess.pure[Try])

      uploader.upload(updatedCuratedTriples) shouldBe InvalidUpdatesFailure(
        curatedTriples.updates.map(_ => exception.getMessage).mkString("; ")
      ).pure[Try]
    }

    s"return $RecoverableFailure if uploading updates fails with such a failure" in new TestCase {

      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(DeliverySuccess.pure[Try])

      val failure = RecoverableFailure(nonEmptyStrings().generateOne)

      curatedTriples.updates.map(
        update =>
          update().fold(throw _,
                        query =>
                          (updatesUploader.send _)
                            .expects(query)
                            .returning(failure.pure[Try]))
      )

      uploader.upload(curatedTriples) shouldBe failure.pure[Try]
    }

    s"return $InvalidTriplesFailure if uploading triples fails with such failure" in new TestCase {

      val failure = InvalidTriplesFailure(nonEmptyStrings().generateOne)
      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(failure.pure[Try])

      uploader.upload(curatedTriples) shouldBe failure.pure[Try]
    }

    s"return $InvalidUpdatesFailure if uploading updates fails with such failure" in new TestCase {

      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(DeliverySuccess.pure[Try])

      val failureMessage = nonEmptyStrings().generateOne
      curatedTriples.updates.map { update =>
        update().fold(throw _,
                      query =>
                        (updatesUploader.send _)
                          .expects(query)
                          .returning(InvalidUpdatesFailure(failureMessage).pure[Try]))
      }

      uploader.upload(curatedTriples) shouldBe InvalidUpdatesFailure(
        curatedTriples.updates.map(_ => failureMessage).mkString("; ")
      ).pure[Try]
    }
  }

  private trait TestCase {
    val curatedTriples = curatedTriplesObjects[Try].generateOne

    val triplesUploader = mock[TriplesUploader[Try]]
    val updatesUploader = mock[UpdatesUploader[Try]]
    val uploader        = new Uploader[Try](triplesUploader, updatesUploader)
  }
}

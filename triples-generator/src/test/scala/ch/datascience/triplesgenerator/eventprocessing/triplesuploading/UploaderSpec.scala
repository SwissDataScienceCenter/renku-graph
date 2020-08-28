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

import cats.MonadError
import cats.data.EitherT
import cats.implicits._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CurationGenerators._
import ch.datascience.triplesgenerator.eventprocessing.triplesuploading.TriplesUploadResult._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Try}

class UploaderSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "upload" should {

    s"return $DeliverySuccess if triples and updates uploading is successful" in new TestCase {

      inSequence {
        (triplesUploader.upload _)
          .expects(curatedTriples.triples)
          .returning(context.pure(DeliverySuccess))

        curatedTriples.updates.map { update =>
          update()
            .fold(throw _,
                  query =>
                    (updatesUploader.send _)
                      .expects(query)
                      .returning(context.pure(DeliverySuccess)))
        }
      }

      uploader.upload(curatedTriples) shouldBe context.pure(DeliverySuccess)
    }

    s"return $RecoverableFailure if triples uploading failed with such failure" in new TestCase {

      val failure = RecoverableFailure("error")
      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(context.pure(failure))

      uploader.upload(curatedTriples) shouldBe context.pure(failure)
    }

    s"return $RecoverableFailure if the given updates fails on query creation" in new TestCase {
      val exception = exceptions.generateOne
      val failure   = RecoverableFailure(exception.getMessage)

      val updatedCuratedTriples = curatedTriples.copy(updates = curatedTriples.updates.map { update =>
        update.copy(
          queryGenerator =
            () => EitherT.leftT[Try, SparqlQuery](new Exception(exception.getMessage) with ProcessingRecoverableError)
        )
      })

      inSequence {
        (triplesUploader.upload _)
          .expects(updatedCuratedTriples.triples)
          .returning(context.pure(DeliverySuccess))
      }
      uploader.upload(updatedCuratedTriples) shouldBe Failure(failure)
    }

    s"return $RecoverableFailure if updates uploading failed with such failure" in new TestCase {

      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(context.pure(DeliverySuccess))

      val failure = RecoverableFailure("error")

      curatedTriples.updates.map(
        update =>
          update().fold(err => throw err,
                        query =>
                          (updatesUploader.send _)
                            .expects(query)
                            .returning(context.pure(failure)))
      )

      uploader.upload(curatedTriples) shouldBe context.pure(failure)
    }

    s"return $InvalidTriplesFailure if triples uploading failed with such failure" in new TestCase {

      val failure = InvalidTriplesFailure("error")
      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(context.pure(failure))

      uploader.upload(curatedTriples) shouldBe context.pure(failure)
    }

    s"return $InvalidUpdatesFailure if updates uploading failed with such failure" in new TestCase {

      (triplesUploader.upload _)
        .expects(curatedTriples.triples)
        .returning(context.pure(DeliverySuccess))

      val failureMessage    = "error"
      val aggregatedFailure = InvalidUpdatesFailure(curatedTriples.updates.map(_ => "error").mkString("; "))
      val failure           = InvalidUpdatesFailure(failureMessage)
      curatedTriples.updates.map { update =>
        update().fold(err => throw err,
                      query =>
                        (updatesUploader.send _)
                          .expects(query)
                          .returning(context.pure(failure)))
      }

      uploader.upload(curatedTriples) shouldBe context.pure(aggregatedFailure)
    }
  }

  private trait TestCase {
    val context = MonadError[Try, Throwable]

    val curatedTriples = curatedTriplesObjects[Try].generateOne

    val triplesUploader = mock[TriplesUploader[Try]]
    val updatesUploader = mock[UpdatesUploader[Try]]
    val uploader        = new Uploader[Try](triplesUploader, updatesUploader)
  }

}

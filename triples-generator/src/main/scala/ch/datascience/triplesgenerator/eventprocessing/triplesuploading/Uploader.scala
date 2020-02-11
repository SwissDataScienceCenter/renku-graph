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
import cats.effect.{ContextShift, Timer}
import cats.implicits._
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class Uploader[Interpretation[_]](
    triplesUploader: TriplesUploader[Interpretation],
    updatesUploader: UpdatesUploader[Interpretation]
)(implicit ME:       MonadError[Interpretation, Throwable]) {

  import TriplesUploadResult._

  def upload(curatedTriples: CuratedTriples): Interpretation[TriplesUploadResult] =
    for {
      triplesUploadingResult <- triplesUploader upload curatedTriples.triples
      maybeUpdatesSendingResult <- if (triplesUploadingResult.failure) ME.pure(Option.empty[TriplesUploadResult])
                                  else updatesUploader send curatedTriples.updates map Option.apply
    } yield merge(triplesUploadingResult, maybeUpdatesSendingResult)

  private lazy val merge: (TriplesUploadResult, Option[TriplesUploadResult]) => TriplesUploadResult = {
    case (DeliverySuccess, Some(DeliverySuccess)) => DeliverySuccess
    case (DeliverySuccess, Some(updatesFailure))  => updatesFailure
    case (triplesFailure, _)                      => triplesFailure
  }
}

object IOUploader {
  import cats.effect.IO

  def apply(
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]): IO[Uploader[IO]] =
    for {
      rdfStoreConfig <- RdfStoreConfig[IO]()
    } yield new Uploader[IO](
      new IOTriplesUploader(rdfStoreConfig, logger),
      new IOUpdatesUploader(rdfStoreConfig, logger, timeRecorder)
    )
}

sealed trait TriplesUploadResult extends Product with Serializable {
  val failure: Boolean
  val message: String
}
object TriplesUploadResult {
  final case object DeliverySuccess extends TriplesUploadResult {
    val failure: Boolean = false
    val message: String  = "Delivery success"
  }
  type DeliverySuccess = DeliverySuccess.type
  sealed trait TriplesUploadFailure extends TriplesUploadResult { val failure: Boolean = true }
  final case class DeliveryFailure(message:       String) extends Exception(message) with TriplesUploadFailure
  final case class InvalidTriplesFailure(message: String) extends Exception(message) with TriplesUploadFailure
  final case class InvalidUpdatesFailure(message: String) extends Exception(message) with TriplesUploadFailure
}

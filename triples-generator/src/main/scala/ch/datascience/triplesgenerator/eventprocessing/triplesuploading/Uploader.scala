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
import cats.syntax.all._
import ch.datascience.logging.ExecutionTimeRecorder
import ch.datascience.rdfstore.{CypherQuery, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.CurationUpdatesGroup
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

class Uploader[Interpretation[_]](
    graphUploader:        GraphUploader[Interpretation],
    triplesUploader:      TriplesUploader[Interpretation],
    updatesUploader:      UpdatesUploader[Interpretation],
    graphUpdatesUploader: GraphUpdatesUploader[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable]) {

  import TriplesUploadResult._

  def upload(curatedTriples:      CuratedTriples[Interpretation, SparqlQuery],
             graphCuratedTriples: CuratedTriples[Interpretation, CypherQuery]
  ): Interpretation[TriplesUploadResult] =
    for {
      triplesUploadingResult <- triplesUploader upload curatedTriples.triples
      _                      <- graphUploader upload graphCuratedTriples.triples
      maybeUpdatesSendingResult <- prepareAndRun(
                                     curatedTriples.updatesGroups,
                                     when = triplesUploadingResult.failure
                                   )
      _ <- prepareAndRunNeo4j(
             graphCuratedTriples.updatesGroups,
             when = triplesUploadingResult.failure
           )
    } yield merge(triplesUploadingResult, maybeUpdatesSendingResult)

  private def prepareAndRun(
      updatesGroups: List[CurationUpdatesGroup[Interpretation, SparqlQuery]],
      when:          Boolean
  ): Interpretation[Option[TriplesUploadResult]] =
    if (when) Option.empty[TriplesUploadResult].pure[Interpretation]
    else
      updatesGroups
        .map(createUpdatesAndSend)
        .flatSequence
        .map(mergeResults) map Option.apply

  private def prepareAndRunNeo4j(
      updatesGroups: List[CurationUpdatesGroup[Interpretation, CypherQuery]],
      when:          Boolean
  ): Interpretation[Option[TriplesUploadResult]] =
    if (when) Option.empty[TriplesUploadResult].pure[Interpretation]
    else
      updatesGroups
        .map(createUpdatesAndSendNeo4j)
        .flatSequence
        .map(mergeResults) map Option.apply

  private def createUpdatesAndSend(
      updatesGroup: CurationUpdatesGroup[Interpretation, SparqlQuery]
  ): Interpretation[List[TriplesUploadResult]] =
    updatesGroup
      .generateUpdates()
      .foldF(
        recoverableError =>
          List(RecoverableFailure(recoverableError.getMessage): TriplesUploadResult).pure[Interpretation],
        queries => (queries map updatesUploader.send).sequence
      ) recoverWith invalidUpdatesFailure

  private def createUpdatesAndSendNeo4j(
      updatesGroup: CurationUpdatesGroup[Interpretation, CypherQuery]
  ): Interpretation[List[TriplesUploadResult]] =
    updatesGroup
      .generateUpdates()
      .foldF(
        recoverableError =>
          List(RecoverableFailure(recoverableError.getMessage): TriplesUploadResult).pure[Interpretation],
        queries => (queries map graphUpdatesUploader.send).sequence
      ) recoverWith invalidUpdatesFailure

  private lazy val invalidUpdatesFailure: PartialFunction[Throwable, Interpretation[List[TriplesUploadResult]]] = {
    case NonFatal(exception) =>
      List(InvalidUpdatesFailure(exception.getMessage): TriplesUploadResult).pure[Interpretation]
  }

  private lazy val merge: (TriplesUploadResult, Option[TriplesUploadResult]) => TriplesUploadResult = {
    case (DeliverySuccess, Some(DeliverySuccess)) => DeliverySuccess
    case (DeliverySuccess, Some(updatesFailure))  => updatesFailure
    case (triplesFailure, _)                      => triplesFailure
  }

  private def mergeResults(results: List[TriplesUploadResult]): TriplesUploadResult =
    results.filterNot(_ == DeliverySuccess) match {
      case Nil => DeliverySuccess
      case failures =>
        failures.find {
          case _: RecoverableFailure => true
          case _ => false
        } match {
          case Some(recoverableFailure) => recoverableFailure
          case _                        => InvalidUpdatesFailure(failures.map(_.message).mkString("; "))
        }
    }
}

private[eventprocessing] object IOUploader {

  import cats.effect.IO

  def apply(
      logger:                  Logger[IO],
      timeRecorder:            SparqlQueryTimeRecorder[IO],
      graphTimeRecorder:       ExecutionTimeRecorder[IO]
  )(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO]): IO[Uploader[IO]] =
    for {
      rdfStoreConfig <- RdfStoreConfig[IO]()
    } yield new Uploader[IO](
      new IOGraphUploader(logger, graphTimeRecorder),
      new IOTriplesUploader(rdfStoreConfig, logger),
      new IOUpdatesUploader(rdfStoreConfig, logger, timeRecorder),
      new IOGraphUpdatesUploader(logger, graphTimeRecorder)
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

  sealed trait TriplesUploadFailure extends TriplesUploadResult {
    val failure: Boolean = true
  }

  final case class RecoverableFailure(message: String) extends Exception(message) with TriplesUploadFailure

  final case class InvalidTriplesFailure(message: String) extends Exception(message) with TriplesUploadFailure

  final case class InvalidUpdatesFailure(message: String) extends Exception(message) with TriplesUploadFailure

}

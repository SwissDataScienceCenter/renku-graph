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

package io.renku.triplesgenerator.events.consumers
package tsprovisioning
package minprojectinfo

import ProcessingRecoverableError.{LogWorthyRecoverableError, SilentRecoverableError}
import cats.{MonadThrow, NonEmptyParallel, Parallel}
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.metrics.{Histogram, MetricsRegistry}
import io.renku.triplesgenerator
import io.renku.triplesgenerator.api.events.ProjectActivated
import io.renku.triplesstore.SparqlQueryTimeRecorder
import org.typelevel.log4cats.Logger
import transformation.TransformationStepsCreator
import triplesuploading.{TransformationStepsRunner, TriplesUploadResult}
import triplesuploading.TriplesUploadResult.{DeliverySuccess, NonRecoverableFailure, RecoverableFailure}

import scala.util.control.NonFatal

private trait EventProcessor[F[_]] {
  def process(event: MinProjectInfoEvent): F[Unit]
}

private class EventProcessorImpl[F[_]: MonadThrow: AccessTokenFinder: Logger](
    stepsCreator:            TransformationStepsCreator[F],
    uploader:                TransformationStepsRunner[F],
    entityBuilder:           EntityBuilder[F],
    projectExistenceChecker: ProjectExistenceChecker[F],
    tgClient:                triplesgenerator.api.events.Client[F],
    executionTimeRecorder:   ExecutionTimeRecorder[F]
) extends EventProcessor[F] {

  private val accessTokenFinder: AccessTokenFinder[F] = AccessTokenFinder[F]
  import EventUploadingResult._
  import accessTokenFinder._
  import entityBuilder._
  import executionTimeRecorder._
  import projectExistenceChecker._
  import stepsCreator._
  import uploader._

  override def process(event: MinProjectInfoEvent): F[Unit] = {
    for {
      _                                   <- Logger[F].info(s"${prefix(event)} accepted")
      implicit0(mat: Option[AccessToken]) <- findAccessToken(event.project.slug)
      result                              <- measureExecutionTime(transformAndUpload(event))
      _                                   <- logSummary(event)(result)
      _                                   <- sendProjectActivatedEvent(event)(result)
    } yield ()
  } recoverWith logError(event)

  private def logError(event: MinProjectInfoEvent): PartialFunction[Throwable, F[Unit]] = { case NonFatal(exception) =>
    Logger[F].error(exception)(show"$categoryName: $event processing failure")
  }

  private def transformAndUpload(
      event: MinProjectInfoEvent
  )(implicit mat: Option[AccessToken]): F[EventUploadingResult] =
    (buildEntity(event) leftSemiflatMap toUploadingError(event)).semiflatMap { project =>
      checkProjectExists(project.resourceId) >>= {
        case true  => Skipped(event).widen.pure[F]
        case false => run(createSteps, project) >>= toUploadingResult(event)
      }
    }.merge recoverWith nonRecoverableFailure(event)

  private def toUploadingError(event: MinProjectInfoEvent): PartialFunction[Throwable, F[EventUploadingResult]] = {
    case error: LogWorthyRecoverableError =>
      Logger[F]
        .error(error)(s"${prefix(event)} ${error.getMessage}")
        .map(_ => RecoverableError(event, error))
    case error: SilentRecoverableError =>
      RecoverableError(event, error).pure[F].widen[EventUploadingResult]
  }

  private def toUploadingResult(event: MinProjectInfoEvent): TriplesUploadResult => F[EventUploadingResult] = {
    case DeliverySuccess => Uploaded(event).pure[F].widen
    case RecoverableFailure(error @ LogWorthyRecoverableError(message, _)) =>
      Logger[F]
        .error(error)(s"${prefix(event)} $message")
        .map(_ => RecoverableError(event, error))
    case RecoverableFailure(error @ SilentRecoverableError(_, _)) =>
      RecoverableError(event, error).pure[F].widen[EventUploadingResult]
    case error: NonRecoverableFailure =>
      Logger[F]
        .error(error)(s"${prefix(event)} ${error.message}")
        .map(_ => NonRecoverableError(event, error: Throwable))
  }

  private def nonRecoverableFailure(event: MinProjectInfoEvent): PartialFunction[Throwable, F[EventUploadingResult]] = {
    case exception: ProcessingNonRecoverableError.MalformedRepository =>
      NonRecoverableError(event, exception).pure[F].widen[EventUploadingResult]
    case NonFatal(exception) =>
      Logger[F]
        .error(exception)(s"${prefix(event)} ${exception.getMessage}")
        .map(_ => NonRecoverableError(event, exception))
  }

  private def sendProjectActivatedEvent(
      event: MinProjectInfoEvent
  ): ((ElapsedTime, EventUploadingResult)) => F[Unit] = {
    case (_, Uploaded(_)) =>
      tgClient
        .send(ProjectActivated.forProject(event.project.slug))
        .handleErrorWith(Logger[F].error(_)(s"${prefix(event)} sending ${ProjectActivated.categoryName} event failed"))
    case _ => ().pure[F]
  }

  private def logSummary(event: MinProjectInfoEvent): ((ElapsedTime, EventUploadingResult)) => F[Unit] = {
    case (elapsedTime, uploadingResult) =>
      val message = uploadingResult match {
        case Uploaded(_) => "success"
        case Skipped(_)  => "skipped"
        case _           => "failure"
      }
      Logger[F].info(s"${prefix(event)} processed in ${elapsedTime}ms: $message")
  }

  private def prefix(event: MinProjectInfoEvent): String = show"$categoryName: $event"

  private sealed trait EventUploadingResult extends UploadingResult with Product {
    val event: MinProjectInfoEvent
    val widen: EventUploadingResult = this
  }
  private sealed trait ProcessingSuccessful extends EventUploadingResult
  private sealed trait UploadingError extends EventUploadingResult {
    val cause: Throwable
  }

  private object EventUploadingResult {
    case class Skipped(event: MinProjectInfoEvent)  extends ProcessingSuccessful with UploadingResult.Uploaded
    case class Uploaded(event: MinProjectInfoEvent) extends ProcessingSuccessful with UploadingResult.Uploaded

    case class RecoverableError(event: MinProjectInfoEvent, cause: ProcessingRecoverableError)
        extends UploadingError
        with UploadingResult.RecoverableError

    case class NonRecoverableError(event: MinProjectInfoEvent, cause: Throwable)
        extends UploadingError
        with UploadingResult.NonRecoverableError
  }
}

private object EventProcessor {

  import eu.timepit.refined.auto._

  def apply[F[
      _
  ]: Async: NonEmptyParallel: Parallel: GitLabClient: AccessTokenFinder: Logger: MetricsRegistry: SparqlQueryTimeRecorder]
      : F[EventProcessor[F]] = for {
    uploader                <- TransformationStepsRunner[F]
    stepsCreator            <- TransformationStepsCreator[F]
    entityBuilder           <- EntityBuilder[F]
    projectExistenceChecker <- ProjectExistenceChecker[F]
    tgClient                <- triplesgenerator.api.events.Client[F]
    eventsProcessingTimes <- Histogram(
                               name = "min_project_info_processing_times",
                               help = "Min project info processing times",
                               buckets = Seq(.1, .5, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000,
                                             1000000, 5000000, 10000000, 50000000, 100000000, 500000000)
                             )
    executionTimeRecorder <- ExecutionTimeRecorder[F](maybeHistogram = Some(eventsProcessingTimes))
  } yield new EventProcessorImpl(stepsCreator,
                                 uploader,
                                 entityBuilder,
                                 projectExistenceChecker,
                                 tgClient,
                                 executionTimeRecorder
  )
}

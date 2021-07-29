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

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ConcurrentEffect, Timer}
import cats.syntax.all._
import ch.datascience.graph.config.GitLabUrlLoader
import ch.datascience.graph.model.{GitLabApiUrl, GitLabUrl}
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.{ProjectMetadata, TransformationStep}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[triplesgenerated] trait TransformationStepsRunner[Interpretation[_]] {
  def run(steps:           List[TransformationStep[Interpretation]],
          projectMetadata: ProjectMetadata
  ): Interpretation[TriplesUploadResult]
}

private[triplesgenerated] class TransformationStepsRunnerImpl[Interpretation[_]: MonadThrow](
    triplesUploader: TriplesUploader[Interpretation],
    updatesUploader: UpdatesUploader[Interpretation],
    gitLabUrl:       GitLabUrl
) extends TransformationStepsRunner[Interpretation] {

  private implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrl.apiV4

  import TriplesUploadResult._

  override def run(steps:           List[TransformationStep[Interpretation]],
                   projectMetadata: ProjectMetadata
  ): Interpretation[TriplesUploadResult] =
    runAllSteps(projectMetadata, steps) >>= {
      case (updatedMetadata, _: DeliverySuccess) => encodeAndSend(updatedMetadata)
      case (_, failure) => failure.pure[Interpretation]
    }

  private def runAllSteps(projectMetadata: ProjectMetadata,
                          steps:           List[TransformationStep[Interpretation]]
  ): Interpretation[(ProjectMetadata, TriplesUploadResult)] =
    steps.foldLeft(
      (projectMetadata, DeliverySuccess: TriplesUploadResult).pure[Interpretation]
    )((lastStepResults, nextStep) =>
      lastStepResults >>= {
        case (_, _: TriplesUploadFailure) => lastStepResults
        case (previousMetadata, _) => runSingleStep(nextStep, previousMetadata)
      }
    )

  private def runSingleStep(nextStep: TransformationStep[Interpretation], previousMetadata: ProjectMetadata) = {
    for {
      stepResults    <- nextStep run previousMetadata
      sendingResults <- EitherT.right[ProcessingRecoverableError](execute(stepResults.queries))
    } yield stepResults.projectMetadata -> sendingResults
  }
    .leftMap(recoverableFailure =>
      previousMetadata -> (RecoverableFailure(recoverableFailure.getMessage): TriplesUploadResult)
    )
    .merge
    .recoverWith(transformationFailure(previousMetadata, nextStep))

  private def execute(queries: List[SparqlQuery]) =
    queries
      .foldLeft((DeliverySuccess: TriplesUploadResult).pure[Interpretation]) { (lastResult, query) =>
        lastResult >>= {
          case _: DeliverySuccess => updatesUploader send query
          case _ => lastResult
        }
      }

  private def transformationFailure(
      metadata:           ProjectMetadata,
      transformationStep: TransformationStep[Interpretation]
  ): PartialFunction[Throwable, Interpretation[(ProjectMetadata, TriplesUploadResult)]] = { case NonFatal(exception) =>
    (metadata,
     InvalidUpdatesFailure(s"${transformationStep.name} transformation step failed: $exception"): TriplesUploadResult
    ).pure[Interpretation]
  }

  private def encodeAndSend(metadata: ProjectMetadata) = {
    val m = metadata.encodeAsFlattenedJsonLD
    m.leftMap(error =>
      InvalidTriplesFailure(s"Metadata for project ${metadata.project.path} failed: ${error.getMessage}")
        .pure[Interpretation]
        .widen[TriplesUploadResult]
    ).map(triplesUploader.upload)
      .merge
  }
}

private[triplesgenerated] object TransformationStepsRunner {

  import cats.effect.IO

  def apply(
      logger:       Logger[IO],
      timeRecorder: SparqlQueryTimeRecorder[IO]
  )(implicit
      executionContext: ExecutionContext,
      concurrentEffect: ConcurrentEffect[IO],
      timer:            Timer[IO]
  ): IO[TransformationStepsRunnerImpl[IO]] = for {
    rdfStoreConfig <- RdfStoreConfig[IO]()
    gitlabUrl      <- GitLabUrlLoader[IO]()
  } yield new TransformationStepsRunnerImpl[IO](new TriplesUploaderImpl[IO](rdfStoreConfig, logger, timeRecorder),
                                                new UpdatesUploaderImpl(rdfStoreConfig, logger, timeRecorder),
                                                gitlabUrl
  )
}

sealed trait TriplesUploadResult extends Product with Serializable {
  val message: String
}

private[triplesgenerated] object TriplesUploadResult {

  type DeliverySuccess = DeliverySuccess.type
  final case object DeliverySuccess extends TriplesUploadResult {
    val message: String = "Delivery success"
  }

  sealed trait TriplesUploadFailure extends TriplesUploadResult
  final case class RecoverableFailure(message: String) extends Exception(message) with TriplesUploadFailure
  final case class InvalidTriplesFailure(message: String) extends Exception(message) with TriplesUploadFailure
  final case class InvalidUpdatesFailure(message: String) extends Exception(message) with TriplesUploadFailure
}

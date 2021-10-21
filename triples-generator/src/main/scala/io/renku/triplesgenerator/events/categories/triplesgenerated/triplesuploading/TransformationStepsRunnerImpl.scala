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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplesuploading

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ConcurrentEffect, Timer}
import cats.syntax.all._
import io.renku.graph.config.{GitLabUrlLoader, RenkuBaseUrlLoader}
import io.renku.graph.model.entities.Project
import io.renku.graph.model.{GitLabApiUrl, GitLabUrl, RenkuBaseUrl}
import io.renku.rdfstore.{RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[triplesgenerated] trait TransformationStepsRunner[F[_]] {
  def run(steps: List[TransformationStep[F]], project: Project): F[TriplesUploadResult]
}

private[triplesgenerated] class TransformationStepsRunnerImpl[F[_]: MonadThrow](
    triplesUploader: TriplesUploader[F],
    updatesUploader: UpdatesUploader[F],
    renkuBaseUrl:    RenkuBaseUrl,
    gitLabUrl:       GitLabUrl
) extends TransformationStepsRunner[F] {

  private implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrl.apiV4
  private implicit val renkuUrl:     RenkuBaseUrl = renkuBaseUrl

  import TriplesUploadResult._
  import io.renku.jsonld.syntax._

  override def run(steps: List[TransformationStep[F]], project: Project): F[TriplesUploadResult] =
    runAllSteps(project, steps) >>= {
      case (updatedProject, _: DeliverySuccess) => encodeAndSend(updatedProject)
      case (_, failure)                         => failure.pure[F]
    }

  private def runAllSteps(project: Project, steps: List[TransformationStep[F]]): F[(Project, TriplesUploadResult)] =
    steps.foldLeft((project, DeliverySuccess: TriplesUploadResult).pure[F])((lastStepResults, nextStep) =>
      lastStepResults >>= {
        case (_, _: TriplesUploadFailure) => lastStepResults
        case (previousMetadata, _)        => runSingleStep(nextStep, previousMetadata)
      }
    )

  private def runSingleStep(nextStep: TransformationStep[F], previousProject: Project) = {
    for {
      stepResults    <- nextStep run previousProject
      sendingResults <- EitherT.right[ProcessingRecoverableError](execute(stepResults.queries))
    } yield stepResults.project -> sendingResults
  }
    .leftMap(recoverableFailure =>
      previousProject -> (RecoverableFailure(recoverableFailure.getMessage): TriplesUploadResult)
    )
    .merge
    .recoverWith(transformationFailure(previousProject, nextStep))

  private def execute(queries: List[SparqlQuery]) =
    queries
      .foldLeft((DeliverySuccess: TriplesUploadResult).pure[F]) { (lastResult, query) =>
        lastResult >>= {
          case _: DeliverySuccess => updatesUploader send query
          case _ => lastResult
        }
      }

  private def transformationFailure(
      project:            Project,
      transformationStep: TransformationStep[F]
  ): PartialFunction[Throwable, F[(Project, TriplesUploadResult)]] = { case NonFatal(exception) =>
    (project,
     InvalidUpdatesFailure(s"${transformationStep.name} transformation step failed: $exception"): TriplesUploadResult
    ).pure[F]
  }

  private def encodeAndSend(project: Project) =
    project.asJsonLD.flatten
      .leftMap(error =>
        InvalidTriplesFailure(s"Metadata for project ${project.path} failed: ${error.getMessage}")
          .pure[F]
          .widen[TriplesUploadResult]
      )
      .map(triplesUploader.upload)
      .merge
}

private[triplesgenerated] object TransformationStepsRunner {

  import cats.effect.IO

  def apply(timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext:   ExecutionContext,
      concurrentEffect:   ConcurrentEffect[IO],
      timer:              Timer[IO],
      logger:             Logger[IO]
  ): IO[TransformationStepsRunnerImpl[IO]] = for {
    rdfStoreConfig <- RdfStoreConfig[IO]()
    renkuBaseUrl   <- RenkuBaseUrlLoader[IO]()
    gitlabUrl      <- GitLabUrlLoader[IO]()
  } yield new TransformationStepsRunnerImpl[IO](new TriplesUploaderImpl[IO](rdfStoreConfig, timeRecorder),
                                                new UpdatesUploaderImpl(rdfStoreConfig, timeRecorder),
                                                renkuBaseUrl,
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

  sealed trait TriplesUploadFailure                       extends TriplesUploadResult
  final case class RecoverableFailure(message: String)    extends Exception(message) with TriplesUploadFailure
  final case class InvalidTriplesFailure(message: String) extends Exception(message) with TriplesUploadFailure
  final case class InvalidUpdatesFailure(message: String) extends Exception(message) with TriplesUploadFailure
}

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
import ch.datascience.graph.config.{GitLabUrlLoader, RenkuBaseUrlLoader}
import ch.datascience.graph.model.entities.Project
import ch.datascience.graph.model.{GitLabApiUrl, GitLabUrl, RenkuBaseUrl}
import ch.datascience.rdfstore.{RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

private[triplesgenerated] trait TransformationStepsRunner[Interpretation[_]] {
  def run(steps: List[TransformationStep[Interpretation]], project: Project): Interpretation[TriplesUploadResult]
}

private[triplesgenerated] class TransformationStepsRunnerImpl[Interpretation[_]: MonadThrow](
    triplesUploader: TriplesUploader[Interpretation],
    updatesUploader: UpdatesUploader[Interpretation],
    renkuBaseUrl:    RenkuBaseUrl,
    gitLabUrl:       GitLabUrl
) extends TransformationStepsRunner[Interpretation] {

  private implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrl.apiV4
  private implicit val renkuUrl:     RenkuBaseUrl = renkuBaseUrl

  import TriplesUploadResult._
  import io.renku.jsonld.syntax._

  override def run(steps:   List[TransformationStep[Interpretation]],
                   project: Project
  ): Interpretation[TriplesUploadResult] =
    runAllSteps(project, steps) >>= {
      case (updatedProject, _: DeliverySuccess) => encodeAndSend(updatedProject)
      case (_, failure) => failure.pure[Interpretation]
    }

  private def runAllSteps(project: Project,
                          steps:   List[TransformationStep[Interpretation]]
  ): Interpretation[(Project, TriplesUploadResult)] =
    steps.foldLeft((project, DeliverySuccess: TriplesUploadResult).pure[Interpretation])((lastStepResults, nextStep) =>
      lastStepResults >>= {
        case (_, _: TriplesUploadFailure) => lastStepResults
        case (previousMetadata, _) => runSingleStep(nextStep, previousMetadata)
      }
    )

  private def runSingleStep(nextStep: TransformationStep[Interpretation], previousProject: Project) = {
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
      .foldLeft((DeliverySuccess: TriplesUploadResult).pure[Interpretation]) { (lastResult, query) =>
        lastResult >>= {
          case _: DeliverySuccess => updatesUploader send query
          case _ => lastResult
        }
      }

  private def transformationFailure(
      project:            Project,
      transformationStep: TransformationStep[Interpretation]
  ): PartialFunction[Throwable, Interpretation[(Project, TriplesUploadResult)]] = { case NonFatal(exception) =>
    (project,
     InvalidUpdatesFailure(s"${transformationStep.name} transformation step failed: $exception"): TriplesUploadResult
    ).pure[Interpretation]
  }

  private def encodeAndSend(project: Project) =
    project.asJsonLD.flatten
      .leftMap(error =>
        InvalidTriplesFailure(s"Metadata for project ${project.path} failed: ${error.getMessage}")
          .pure[Interpretation]
          .widen[TriplesUploadResult]
      )
      .map(triplesUploader.upload)
      .merge
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
    renkuBaseUrl   <- RenkuBaseUrlLoader[IO]()
    gitlabUrl      <- GitLabUrlLoader[IO]()
  } yield new TransformationStepsRunnerImpl[IO](new TriplesUploaderImpl[IO](rdfStoreConfig, logger, timeRecorder),
                                                new UpdatesUploaderImpl(rdfStoreConfig, logger, timeRecorder),
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

  sealed trait TriplesUploadFailure extends TriplesUploadResult
  final case class RecoverableFailure(message: String) extends Exception(message) with TriplesUploadFailure
  final case class InvalidTriplesFailure(message: String) extends Exception(message) with TriplesUploadFailure
  final case class InvalidUpdatesFailure(message: String) extends Exception(message) with TriplesUploadFailure
}

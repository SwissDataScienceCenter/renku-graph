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
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.config.{GitLabUrlLoader, RenkuBaseUrlLoader}
import io.renku.graph.model.entities.Project
import io.renku.graph.model.{GitLabApiUrl, GitLabUrl, RenkuBaseUrl}
import io.renku.rdfstore.{RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep
import io.renku.triplesgenerator.events.categories.triplesgenerated.TransformationStep.{ProjectWithQueries, Queries}
import io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.TransformationStepsCreator.TransformationRecoverableError
import org.typelevel.log4cats.Logger

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

  override def run(steps: List[TransformationStep[F]], project: Project): F[TriplesUploadResult] = {
    runAll(steps)(project) >>=
      executeAllPreDataUploadQueries >>=
      encodeAndSendProject >>=
      executeAllPostDataUploadQueries
  }
    .leftMap(recoverableFailure => RecoverableFailure(recoverableFailure.getMessage))
    .map(_ => DeliverySuccess)
    .leftWiden[TriplesUploadResult]
    .merge
    .recoverWith(transformationFailure(project))
    .widen[TriplesUploadResult]

  private def runAll(steps: List[TransformationStep[F]])(project: Project): ProjectWithQueries[F] =
    steps.foldLeft(EitherT.rightT[F, ProcessingRecoverableError](project -> Queries.empty))(
      (previousProjectWithQueries, step) =>
        previousProjectWithQueries >>= { case (project, queries) =>
          (step run project).map { case (updatedProject, stepQueries) => updatedProject -> (queries |+| stepQueries) }
        }
    )

  private lazy val executeAllPreDataUploadQueries: ((Project, Queries)) => ProjectWithQueries[F] = {
    case projectAndQueries @ (_, Queries(preQueries, _)) =>
      execute(preQueries).bimap(
        error => TransformationRecoverableError(s"Failed to execute pre-data-upload queries: ${error.message}", error),
        _ => projectAndQueries
      )
  }

  private lazy val encodeAndSendProject: ((Project, Queries)) => ProjectWithQueries[F] = {
    case projectAndQueries @ (project, _) =>
      for {
        jsonLD <- EitherT
                    .fromEither[F](project.asJsonLD.flatten)
                    .leftSemiflatMap(error =>
                      NonRecoverableFailure(s"Metadata for project ${project.path} failed: ${error.getMessage}", error)
                        .raiseError[F, ProcessingRecoverableError]
                    )
        _ <- triplesUploader
               .uploadTriples(jsonLD)
               .leftMap(error => TransformationRecoverableError(s"Failed to upload json-ld: ${error.message}", error))
               .leftWiden[ProcessingRecoverableError]
      } yield projectAndQueries
  }

  private lazy val executeAllPostDataUploadQueries: ((Project, Queries)) => ProjectWithQueries[F] = {
    case projectAndQueries @ (_, Queries(_, postQueries)) =>
      execute(postQueries).bimap(
        error => TransformationRecoverableError(s"Failed to execute post-data-upload queries: ${error.message}", error),
        _ => projectAndQueries
      )
  }

  private def execute(queries: List[SparqlQuery]): EitherT[F, RecoverableFailure, DeliverySuccess] =
    queries.foldLeft(EitherT.rightT[F, RecoverableFailure](DeliverySuccess)) { (previousResult, query) =>
      previousResult >> updatesUploader.send(query)
    }

  private def transformationFailure(
      project: Project
  ): PartialFunction[Throwable, F[TriplesUploadResult]] = { case NonFatal(exception) =>
    NonRecoverableFailure(s"Transformation of ${project.path} failed: $exception", exception)
      .pure[F]
      .widen[TriplesUploadResult]
  }
}

private[triplesgenerated] object TransformationStepsRunner {

  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[TransformationStepsRunnerImpl[F]] = for {
    rdfStoreConfig <- RdfStoreConfig[F]()
    renkuBaseUrl   <- RenkuBaseUrlLoader[F]()
    gitlabUrl      <- GitLabUrlLoader[F]()
  } yield new TransformationStepsRunnerImpl[F](new TriplesUploaderImpl[F](rdfStoreConfig, timeRecorder),
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

  sealed trait TriplesUploadFailure                    extends TriplesUploadResult
  final case class RecoverableFailure(message: String) extends Exception(message) with TriplesUploadFailure
  sealed trait NonRecoverableFailure                   extends Exception with TriplesUploadFailure
  object NonRecoverableFailure {
    case class NonRecoverableFailureWithCause(message: String, cause: Throwable)
        extends Exception(message, cause)
        with NonRecoverableFailure
    case class NonRecoverableFailureWithoutCause(message: String) extends Exception(message) with NonRecoverableFailure

    def apply(message: String, cause: Throwable): NonRecoverableFailure = NonRecoverableFailureWithCause(message, cause)
    def apply(message: String):                   NonRecoverableFailure = NonRecoverableFailureWithoutCause(message)

    def unapply(nonRecoverableFailure: NonRecoverableFailure): Option[(String, Option[Throwable])] =
      nonRecoverableFailure match {
        case NonRecoverableFailureWithCause(message, cause) => Some(message, Some(cause))
        case NonRecoverableFailureWithoutCause(message)     => Some(message, None)
      }
  }
}

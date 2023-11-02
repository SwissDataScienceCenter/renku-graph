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

package io.renku.triplesgenerator.tsprovisioning
package triplesuploading

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.{RenkuUrl, datasets}
import io.renku.graph.model.entities.Project
import io.renku.lock.Lock
import io.renku.projectauth.{ProjectAuthData, ProjectMember}
import io.renku.triplesgenerator.errors.ProcessingRecoverableError
import io.renku.triplesgenerator.events.consumers.membersync.ProjectAuthSync
import io.renku.triplesgenerator.tsprovisioning.TransformationStep.{ProjectWithQueries, Queries}
import io.renku.triplesstore.{ProjectSparqlClient, SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private[tsprovisioning] trait TransformationStepsRunner[F[_]] {
  def run(steps: List[TransformationStep[F]], project: Project): F[TriplesUploadResult]
}

private class TransformationStepsRunnerImpl[F[_]: MonadThrow](
    resultsUploader:         TransformationResultsUploader[F],
    searchGraphsProvisioner: SearchGraphsProvisioner[F],
    projectAuthSync:         ProjectAuthSync[F]
) extends TransformationStepsRunner[F] {

  import TriplesUploadResult._

  override def run(steps: List[TransformationStep[F]], project: Project): F[TriplesUploadResult] = {
    runAll(steps)(project) >>=
      executeAllPreDataUploadQueries >>=
      encodeAndSendProject >>=
      executeAllPostDataUploadQueries >>=
      provisionSearchGraphs >>=
      provisionProjectAuthGraph
  }
    .leftMap(RecoverableFailure)
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

  private def executeAllPreDataUploadQueries: ((Project, Queries)) => ProjectWithQueries[F] = {
    case projectAndQueries @ (_, Queries(preQueries, _)) =>
      execute(preQueries) map (_ => projectAndQueries)
  }

  private def encodeAndSendProject: ((Project, Queries)) => ProjectWithQueries[F] = {
    case projectAndQueries @ (project, _) =>
      resultsUploader.upload(project) map (_ => projectAndQueries)
  }

  private def executeAllPostDataUploadQueries: ((Project, Queries)) => ProjectWithQueries[F] = {
    case projectAndQueries @ (_, Queries(_, postQueries)) =>
      execute(postQueries) map (_ => projectAndQueries)
  }

  private def provisionSearchGraphs: ((Project, Queries)) => ProjectWithQueries[F] = {
    case projectAndQueries @ (project, _) =>
      searchGraphsProvisioner.provisionSearchGraphs(project).map(_ => projectAndQueries)
  }

  private def provisionProjectAuthGraph: ((Project, Queries)) => ProjectWithQueries[F] = { case (project, queries) =>
    val authData = ProjectAuthData(
      project.slug,
      project.members.flatMap(m => m.person.maybeGitLabId.map(gId => ProjectMember(gId, m.role))),
      project.visibility
    )
    EitherT
      .liftF[F, ProcessingRecoverableError, Unit](
        projectAuthSync.syncProject(authData)
      )
      .map(_ => (project, queries))
  }

  private def execute(queries: List[SparqlQuery]): EitherT[F, ProcessingRecoverableError, Unit] =
    queries.foldLeft(EitherT.rightT[F, ProcessingRecoverableError](())) { (previousResult, query) =>
      previousResult >> resultsUploader.execute(query)
    }

  private def transformationFailure(project: Project): PartialFunction[Throwable, F[TriplesUploadResult]] = {
    case NonFatal(exception) =>
      NonRecoverableFailure(s"Transformation of ${project.slug} failed: $exception", exception)
        .pure[F]
        .widen[TriplesUploadResult]
  }
}

private[tsprovisioning] object TransformationStepsRunner {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](topSameAsLock:       Lock[F, datasets.TopmostSameAs],
                                                          projectSparqlClient: ProjectSparqlClient[F]
  )(implicit renkuUrl: RenkuUrl): F[TransformationStepsRunner[F]] = for {
    resultsUploader         <- TransformationResultsUploader[F]
    searchGraphsProvisioner <- SearchGraphsProvisioner.default[F](topSameAsLock)
    projectAuthSync = ProjectAuthSync(projectSparqlClient)
  } yield new TransformationStepsRunnerImpl[F](resultsUploader, searchGraphsProvisioner, projectAuthSync)
}

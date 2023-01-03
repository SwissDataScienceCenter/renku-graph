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

package io.renku.triplesgenerator.events.consumers.membersync

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.graph.tokenrepository.AccessTokenFinder
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.logging.ExecutionTimeRecorder
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

import scala.util.control.NonFatal

private trait MembersSynchronizer[F[_]] {
  def synchronizeMembers(path: projects.Path): F[Unit]
}

private class MembersSynchronizerImpl[F[_]: MonadThrow: AccessTokenFinder: Logger](
    glMembersFinder:       GitLabProjectMembersFinder[F],
    kgSynchronizer:        KGSynchronizer[F],
    executionTimeRecorder: ExecutionTimeRecorder[F]
) extends MembersSynchronizer[F] {

  private val accessTokenFinder: AccessTokenFinder[F] = AccessTokenFinder[F]
  import accessTokenFinder._
  import executionTimeRecorder._

  override def synchronizeMembers(path: projects.Path): F[Unit] = {
    for {
      implicit0(mat: Option[AccessToken]) <- findAccessToken(path)
      membersInGL                         <- glMembersFinder.findProjectMembers(path)
      _                                   <- syncMembers(path, kgSynchronizer.syncMembers(_, membersInGL))
    } yield ()
  } recoverWith { case NonFatal(exception) =>
    Logger[F].error(exception)(s"$categoryName: Members synchronized for project $path FAILED")
  }

  private def syncMembers(path: projects.Path, sync: projects.Path => F[SyncSummary]) =
    measureExecutionTime(sync(path)) >>= logSummary(path)

  private def logSummary(path: projects.Path): ((ElapsedTime, SyncSummary)) => F[Unit] = {
    case (elapsedTime, SyncSummary(membersAdded, membersRemoved)) =>
      Logger[F].info(
        s"$categoryName: members for project: $path synchronized in ${elapsedTime}ms: " +
          s"$membersAdded member(s) added, $membersRemoved member(s) removed"
      )
  }
}

private object MembersSynchronizer {
  def apply[F[_]: Async: GitLabClient: AccessTokenFinder: Logger: SparqlQueryTimeRecorder]: F[MembersSynchronizer[F]] =
    for {
      gitLabProjectMembersFinder <- GitLabProjectMembersFinder[F]
      kgSynchronizer             <- namedgraphs.KGSynchronizer[F]
      executionTimeRecorder      <- ExecutionTimeRecorder[F](maybeHistogram = None)
    } yield new MembersSynchronizerImpl[F](gitLabProjectMembersFinder, kgSynchronizer, executionTimeRecorder)
}

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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import cats.{Applicative, MonadThrow}
import io.circe.DecodingFailure
import io.renku.config.GitLab
import io.renku.control.Throttler
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.RenkuBaseUrl
import io.renku.graph.model.entities.Project.GitLabProjectInfo
import io.renku.graph.model.entities._
import io.renku.http.client.AccessToken
import io.renku.jsonld.JsonLDDecoder.decodeList
import io.renku.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import org.typelevel.log4cats.Logger

private trait JsonLDDeserializer[F[_]] {
  def deserializeToModel(event: TriplesGeneratedEvent)(implicit
      maybeAccessToken:         Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Project]
}

private class JsonLDDeserializerImpl[F[_]: MonadThrow](
    projectInfoFinder: ProjectInfoFinder[F],
    renkuBaseUrl:      RenkuBaseUrl
) extends JsonLDDeserializer[F] {

  private implicit val renkuUrl: RenkuBaseUrl   = renkuBaseUrl
  private val applicative:       Applicative[F] = Applicative[F]

  import applicative._
  import projectInfoFinder._

  override def deserializeToModel(event: TriplesGeneratedEvent)(implicit
      maybeAccessToken:                  Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Project] = for {
    projectInfo <- findValidProjectInfo(event)
    project     <- extractProject(projectInfo, event)
  } yield project

  private def findValidProjectInfo(event: TriplesGeneratedEvent)(implicit
      maybeAccessToken:                   Option[AccessToken]
  ) = findProjectInfo(event.project.path) semiflatMap {
    case Some(projectInfo) => projectInfo.pure[F]
    case None =>
      new IllegalStateException(s"No project ${event.project.show} found in GitLab")
        .raiseError[F, GitLabProjectInfo]
  }

  private def extractProject(projectInfo: GitLabProjectInfo, event: TriplesGeneratedEvent) =
    EitherT.right[ProcessingRecoverableError] {
      for {
        projects <- event.payload.cursor
                      .as[List[Project]](decodeList(Project.decoder(projectInfo)))
                      .fold(raiseError(event), _.pure[F])
        project <- projects match {
                     case project :: Nil => project.pure[F]
                     case other =>
                       new IllegalStateException(
                         s"${other.size} Project entities found in the JsonLD for ${event.project.show}"
                       ).raiseError[F, Project]
                   }
        _ <- whenA(event.project.path != project.path)(
               new IllegalStateException(
                 s"Event for project ${event.project.show} contains payload for project ${project.path}"
               ).raiseError[F, Unit]
             )
      } yield project
    }

  private def raiseError[T](event: TriplesGeneratedEvent): DecodingFailure => F[T] =
    err =>
      new IllegalStateException(s"Finding Project entity in the JsonLD for ${event.project.show} failed", err)
        .raiseError[F, T]
}

private object JsonLDDeserializer {
  def apply[F[_]: Async: Logger](gitLabThrottler: Throttler[F, GitLab]): F[JsonLDDeserializer[F]] = for {
    renkuBaseUrl      <- RenkuBaseUrlLoader[F]()
    projectInfoFinder <- ProjectInfoFinder(gitLabThrottler)
  } yield new JsonLDDeserializerImpl[F](projectInfoFinder, renkuBaseUrl)
}

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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.MonadThrow
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.config.GitLab
import ch.datascience.control.Throttler
import ch.datascience.graph.config.RenkuBaseUrlLoader
import ch.datascience.graph.model.RenkuBaseUrl
import ch.datascience.graph.model.entities.Project.GitLabProjectInfo
import ch.datascience.graph.model.entities._
import ch.datascience.http.client.AccessToken
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import io.circe.DecodingFailure
import io.renku.jsonld.JsonLDDecoder.decodeList
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait JsonLDDeserializer[Interpretation[_]] {
  def deserializeToModel(event: TriplesGeneratedEvent)(implicit
      maybeAccessToken:         Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, ProjectMetadata]
}

private class JsonLDDeserializerImpl[Interpretation[_]: MonadThrow](
    projectInfoFinder: ProjectInfoFinder[Interpretation],
    renkuBaseUrl:      RenkuBaseUrl
) extends JsonLDDeserializer[Interpretation] {

  import projectInfoFinder._
  private implicit val renkuUrl: RenkuBaseUrl = renkuBaseUrl

  override def deserializeToModel(event: TriplesGeneratedEvent)(implicit
      maybeAccessToken:                  Option[AccessToken]
  ): EitherT[Interpretation, ProcessingRecoverableError, ProjectMetadata] = for {
    persons     <- extractPersons(event)
    projectInfo <- findValidProjectInfo(event)
    project     <- extractProject(projectInfo, event, persons)
    activities  <- extractActivities(event)
    datasets    <- extractDatasets(event)
    metadata    <- createMetadata(project, activities, datasets)(event)
  } yield metadata

  private def findValidProjectInfo(event: TriplesGeneratedEvent)(implicit
      maybeAccessToken:                   Option[AccessToken]
  ) = findProjectInfo(event.project.path) semiflatMap {
    case Some(projectInfo) => projectInfo.pure[Interpretation]
    case None =>
      new IllegalStateException(s"No project ${event.project.show} found in GitLab")
        .raiseError[Interpretation, GitLabProjectInfo]
  }

  private def extractPersons(event: TriplesGeneratedEvent) = EitherT.right[ProcessingRecoverableError] {
    event.triples.cursor
      .as[List[Person]]
      .fold(
        raiseError(s"Finding Person entities in the JsonLD for ${event.project.show} failed"),
        _.toSet.pure[Interpretation]
      )
  }

  private def extractProject(projectInfo: GitLabProjectInfo, event: TriplesGeneratedEvent, allPersons: Set[Person]) =
    EitherT.right[ProcessingRecoverableError] {
      for {
        projects <- event.triples.cursor
                      .as[List[Project]](decodeList(Project.decoder(projectInfo, allPersons)))
                      .fold(
                        raiseError(s"Finding Project entity in the JsonLD for ${event.project.show} failed"),
                        _.pure[Interpretation]
                      )
        project <- projects match {
                     case project :: Nil => project.pure[Interpretation]
                     case other =>
                       new IllegalStateException(
                         s"${other.size} Project entities found in the JsonLD for ${event.project.show}"
                       )
                         .raiseError[Interpretation, Project]
                   }
      } yield project
    }

  private def extractActivities(event: TriplesGeneratedEvent) = EitherT.right[ProcessingRecoverableError] {
    event.triples.cursor
      .as[List[Activity]]
      .fold(
        raiseError(s"Finding Activity entities in the JsonLD for ${event.project.show} failed"),
        _.pure[Interpretation]
      )
  }

  private def extractDatasets(event: TriplesGeneratedEvent) = EitherT.right[ProcessingRecoverableError] {
    event.triples.cursor
      .as[List[Dataset[Dataset.Provenance]]]
      .fold(
        raiseError(s"Finding Dataset entities in the JsonLD for ${event.project.show} failed"),
        _.pure[Interpretation]
      )
  }

  private def createMetadata(
      project:    Project,
      activities: List[Activity],
      datasets:   List[Dataset[Dataset.Provenance]]
  )(event:        TriplesGeneratedEvent) = EitherT.right[ProcessingRecoverableError] {
    ProjectMetadata
      .from(project, activities, datasets)
      .fold(
        errors =>
          new IllegalStateException(
            s"Invalid payload for project ${event.project.show}: ${errors.nonEmptyIntercalate("; ")}"
          ).raiseError[Interpretation, ProjectMetadata],
        _.pure[Interpretation]
      )
  }

  private def raiseError[T](message: String): DecodingFailure => Interpretation[T] =
    err => new IllegalStateException(message, err).raiseError[Interpretation, T]
}

private object JsonLDDeserializer {
  def apply(
      gitLabThrottler: Throttler[IO, GitLab],
      logger:          Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[JsonLDDeserializer[IO]] = for {
    renkuBaseUrl      <- RenkuBaseUrlLoader[IO]()
    projectInfoFinder <- ProjectInfoFinder(gitLabThrottler, logger)
  } yield new JsonLDDeserializerImpl[IO](projectInfoFinder, renkuBaseUrl)
}

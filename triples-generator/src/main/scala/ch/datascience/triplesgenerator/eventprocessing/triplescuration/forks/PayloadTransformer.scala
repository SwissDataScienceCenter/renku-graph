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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.MonadError
import cats.data.EitherT
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.{ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent
import ch.datascience.triplesgenerator.eventprocessing.CommitEventProcessor.ProcessingRecoverableError
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.IOTriplesCurator.CurationRecoverableError
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext

private[triplescuration] trait PayloadTransformer[Interpretation[_]] {
  def transform(
      commit:                  CommitEvent,
      triples:                 JsonLDTriples
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[Interpretation, ProcessingRecoverableError, JsonLDTriples]
}

private[triplescuration] class PayloadTransformerImpl(
    gitLab:                   GitLabInfoFinder[IO],
    projectPropertiesRemover: JsonLDTriples => JsonLDTriples
)(implicit ME:                MonadError[IO, Throwable], cs: ContextShift[IO])
    extends PayloadTransformer[IO] {

  override def transform(
      commit:                  CommitEvent,
      triples:                 JsonLDTriples
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[IO, ProcessingRecoverableError, JsonLDTriples] = EitherT {
    gitLab
      .findProject(commit.project.path)
      .map {
        case None => triples.pure[IO]
        case _    => projectPropertiesRemover(triples).pure[IO]
      }
      .flatten
      .map(_.asRight[ProcessingRecoverableError])
      .recover(maybeToRecoverableError)
  }

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, Either[ProcessingRecoverableError, JsonLDTriples]] = {
    case e: UnexpectedResponseException =>
      Left[ProcessingRecoverableError, JsonLDTriples](
        CurationRecoverableError("Problem with finding fork info", e)
      )
    case e: ConnectivityException =>
      Left[ProcessingRecoverableError, JsonLDTriples](
        CurationRecoverableError("Problem with finding fork info", e)
      )
  }
}

private[triplescuration] object IOPayloadTransformer {
  import cats.effect.Timer
  import ch.datascience.config.GitLab
  import ch.datascience.control.Throttler

  def apply(
      gitLabThrottler:         Throttler[IO, GitLab],
      logger:                  Logger[IO]
  )(implicit executionContext: ExecutionContext, cs: ContextShift[IO], timer: Timer[IO]): IO[PayloadTransformer[IO]] =
    for {
      gitLabInfoFinder <- IOGitLabInfoFinder(gitLabThrottler, logger)
      triplesTransformer = new ProjectPropertiesRemover
    } yield new PayloadTransformerImpl(gitLabInfoFinder, triplesTransformer)
}

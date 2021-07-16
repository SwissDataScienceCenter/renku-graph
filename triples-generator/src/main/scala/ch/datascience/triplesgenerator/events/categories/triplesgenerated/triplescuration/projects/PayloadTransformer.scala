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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.data.EitherT
import cats.effect.{ContextShift, IO}
import cats.syntax.all._
import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.events.categories.Errors.ProcessingRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.TriplesCurator.CurationRecoverableError
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.{CuratedTriples, TriplesGeneratedEvent}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait PayloadTransformer[Interpretation[_]] {
  def transform(
      event:                   TriplesGeneratedEvent,
      curatedTriples:          CuratedTriples[Interpretation]
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[Interpretation, ProcessingRecoverableError, JsonLDTriples]
}

private class PayloadTransformerImpl(
    gitLab:                   GitLabInfoFinder[IO],
    projectPropertiesRemover: JsonLDTriples => JsonLDTriples
) extends PayloadTransformer[IO] {

  override def transform(
      event:                   TriplesGeneratedEvent,
      curatedTriples:          CuratedTriples[IO]
  )(implicit maybeAccessToken: Option[AccessToken]): EitherT[IO, ProcessingRecoverableError, JsonLDTriples] = EitherT {
    gitLab
      .findProject(event.project.path)
      .map {
        case None => curatedTriples.triples.pure[IO]
        case _    => projectPropertiesRemover(curatedTriples.triples).pure[IO]
      }
      .flatten
      .map(_.asRight[ProcessingRecoverableError])
      .recover(maybeToRecoverableError)
  }

  private lazy val maybeToRecoverableError
      : PartialFunction[Throwable, Either[ProcessingRecoverableError, JsonLDTriples]] = {
    case e @ (_: UnexpectedResponseException | _: ConnectivityException | _: ClientException) =>
      Left[ProcessingRecoverableError, JsonLDTriples](
        CurationRecoverableError("Problem with finding fork info", e)
      )
  }
}

private object IOPayloadTransformer {

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

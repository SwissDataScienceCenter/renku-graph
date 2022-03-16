/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.triplesgenerator.events.categories.triplesgenerated.projectinfo

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.{persons, projects}
import io.renku.http.client.{AccessToken, GitLabClient}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.triplesgenerated.RecoverableErrorsRecovery
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.{EntityDecoder, InvalidMessageBodyFailure, Request, Response, Status}
import org.typelevel.log4cats.Logger

private trait CommitAuthorFinder[F[_]] {
  def findCommitAuthor(projectPath: projects.Path, commitId: CommitId)(implicit
      maybeAccessToken:             Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[(persons.Name, persons.Email)]]
}

private object CommitAuthorFinder {
  def apply[F[_]: Async: Logger](gitLabClient: GitLabClient[F]): F[CommitAuthorFinder[F]] = new CommitAuthorFinderImpl(
    gitLabClient
  ).pure[F].widen
}

private class CommitAuthorFinderImpl[F[_]: Async: Logger](
    gitLabClient:     GitLabClient[F],
    recoveryStrategy: RecoverableErrorsRecovery = RecoverableErrorsRecovery
) extends CommitAuthorFinder[F] {

  import org.http4s.Method.GET
  import org.http4s.dsl.io.{NotFound, Ok}

  override def findCommitAuthor(path: projects.Path, commitId: CommitId)(implicit
      maybeAccessToken:               Option[AccessToken]
  ): EitherT[F, ProcessingRecoverableError, Option[(persons.Name, persons.Email)]] = EitherT {
    gitLabClient
      .send(GET, uri"projects" / path.value / "repository" / "commits" / commitId.show, "commit")(
        mapTo[(persons.Name, persons.Email)]
      )
      .map(_.asRight[ProcessingRecoverableError])
      .recoverWith(recoveryStrategy.maybeRecoverableError)
  }

  private def mapTo[OUT](implicit
      decoder: EntityDecoder[F, OUT]
  ): PartialFunction[(Status, Request[F], Response[F]), F[Option[OUT]]] = {
    case (Ok, _, response) => response.as[OUT].map(Option.apply).recoverWith(noAuthor[OUT])
    case (NotFound, _, _)  => Option.empty[OUT].pure[F]
  }

  private implicit lazy val authorDecoder: EntityDecoder[F, (persons.Name, persons.Email)] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import org.http4s.circe.jsonOf

    implicit val decoder: Decoder[(persons.Name, persons.Email)] = cursor =>
      (
        cursor.downField("author_name").as[persons.Name],
        cursor.downField("author_email").as[persons.Email]
      ).mapN(_ -> _)

    jsonOf[F, (persons.Name, persons.Email)]
  }

  private def noAuthor[OUT]: PartialFunction[Throwable, F[Option[OUT]]] = { case _: InvalidMessageBodyFailure =>
    Option.empty[OUT].pure[F]
  }
}

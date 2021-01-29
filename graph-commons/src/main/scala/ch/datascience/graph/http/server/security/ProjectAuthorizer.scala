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

package ch.datascience.graph.http.server.security

import cats.MonadError
import cats.data.EitherT
import cats.data.EitherT.{leftT, rightT}
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Visibility._
import ch.datascience.graph.model.projects.{Path, ResourceId, Visibility}
import ch.datascience.graph.model.users.GitLabId
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.http.server.security.EndpointSecurityException
import ch.datascience.http.server.security.EndpointSecurityException.AuthorizationFailure
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import io.chrisdavenport.log4cats.Logger
import io.circe.{Decoder, DecodingFailure}

import scala.concurrent.ExecutionContext

trait ProjectAuthorizer[Interpretation[_]] {
  def authorize(path:          projects.Path,
                maybeAuthUser: Option[AuthUser]
  ): EitherT[Interpretation, EndpointSecurityException, Unit]
}

object ProjectAuthorizer {
  def apply(
      timeRecorder:   SparqlQueryTimeRecorder[IO],
      renkuBaseUrl:   IO[RenkuBaseUrl] = RenkuBaseUrl[IO](),
      rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO](),
      logger:         Logger[IO]
  )(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[ProjectAuthorizer[IO]] =
    for {
      config   <- rdfStoreConfig
      renkuUrl <- renkuBaseUrl
    } yield new ProjectAuthorizerImpl(config, renkuUrl, logger, timeRecorder)
}

class ProjectAuthorizerImpl(
    rdfStoreConfig: RdfStoreConfig,
    renkuBaseUrl:   RenkuBaseUrl,
    logger:         Logger[IO],
    timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO],
    ME:               MonadError[IO, Throwable]
) extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with ProjectAuthorizer[IO] {
  override def authorize(path:          projects.Path,
                         maybeAuthUser: Option[AuthUser]
  ): EitherT[IO, EndpointSecurityException, Unit] = for {
    records <- EitherT.right(queryExpecting[List[Record]](using = query(path))(recordsDecoder))
    _       <- validate(maybeAuthUser, records)
  } yield ()

  import ch.datascience.graph.Schemas._
  import eu.timepit.refined.auto._

  private def query(path: Path) = SparqlQuery.of(
    name = "project by id",
    Prefixes.of(rdf -> "rdf", schema -> "schema", renku -> "renku"),
    s"""|SELECT DISTINCT ?projectId ?maybeVisibility (GROUP_CONCAT(?maybeMemberGitLabId; separator=',') AS ?memberGitLabIds)
        |WHERE {
        |  BIND (${ResourceId(renkuBaseUrl, path).showAs[RdfResource]} AS ?projectId)
        |  ?projectId rdf:type schema:Project.
        |  OPTIONAL { ?projectId renku:projectVisibility ?maybeVisibility. }
        |  OPTIONAL {
        |    ?projectId schema:member/schema:sameAs ?sameAsId.
        |    ?sameAsId schema:additionalType 'GitLab';
        |              schema:identifier ?maybeMemberGitLabId.
        |  }
        |}
        |GROUP BY ?projectId ?maybeVisibility
        |""".stripMargin
  )

  private type Record = (Visibility, Set[GitLabId])

  private lazy val recordsDecoder: Decoder[List[Record]] = {
    import Decoder._

    val recordDecoder: Decoder[Record] = { cursor =>
      for {
        maybeVisibility <-
          cursor
            .downField("maybeVisibility")
            .downField("value")
            .as[Option[String]]
            .map(_.map(_.toLowerCase))
            .flatMap {
              case None        => Right(Public)
              case Some(value) => Visibility.from(value).leftMap(ex => DecodingFailure(ex.getMessage, Nil))
            }
        maybeUserId <- cursor
                         .downField("memberGitLabIds")
                         .downField("value")
                         .as[Option[String]]
                         .map(_.map(_.split(",").toList).getOrElse(List.empty))
                         .flatMap(_.map(GitLabId.parse).sequence.leftMap(ex => DecodingFailure(ex.getMessage, Nil)))
                         .map(_.toSet)
      } yield maybeVisibility -> maybeUserId
    }

    _.downField("results").downField("bindings").as(decodeList(recordDecoder))
  }

  private def validate(
      maybeAuthUser: Option[AuthUser],
      records:       List[Record]
  ): EitherT[IO, EndpointSecurityException, Unit] = records -> maybeAuthUser match {
    case (Nil, _)                                                                            => rightT(())
    case ((Public, _) :: Nil, _)                                                             => rightT(())
    case ((_, projectMembers) :: Nil, Some(authUser)) if projectMembers contains authUser.id => rightT(())
    case _                                                                                   => leftT(AuthorizationFailure)
  }
}

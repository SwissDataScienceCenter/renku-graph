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

package io.renku.graph.http.server.security

import cats.data.EitherT
import cats.data.EitherT.{leftT, rightT}
import cats.effect.kernel.Temporal
import cats.effect.{Async, IO}
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.config.RenkuBaseUrlLoader
import io.renku.graph.model.projects.Visibility._
import io.renku.graph.model.projects.{Path, ResourceId, Visibility}
import io.renku.graph.model.users.GitLabId
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{RenkuBaseUrl, projects}
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.AuthUser
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

trait ProjectAuthorizer[Interpretation[_]] {
  def authorize(path:          projects.Path,
                maybeAuthUser: Option[AuthUser]
  ): EitherT[Interpretation, EndpointSecurityException, Unit]
}

object ProjectAuthorizer {
  def apply(
      timeRecorder:   SparqlQueryTimeRecorder[IO],
      renkuBaseUrl:   IO[RenkuBaseUrl] = RenkuBaseUrlLoader[IO](),
      rdfStoreConfig: IO[RdfStoreConfig] = RdfStoreConfig[IO]()
  )(implicit logger:  Logger[IO]): IO[ProjectAuthorizer[IO]] = for {
    config   <- rdfStoreConfig
    renkuUrl <- renkuBaseUrl
  } yield new ProjectAuthorizerImpl(config, renkuUrl, timeRecorder)
}

class ProjectAuthorizerImpl[Interpretation[_]: Async: Temporal: Logger](
    rdfStoreConfig: RdfStoreConfig,
    renkuBaseUrl:   RenkuBaseUrl,
    timeRecorder:   SparqlQueryTimeRecorder[Interpretation]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with ProjectAuthorizer[Interpretation] {

  override def authorize(path:          projects.Path,
                         maybeAuthUser: Option[AuthUser]
  ): EitherT[Interpretation, EndpointSecurityException, Unit] = for {
    records <- EitherT.right(queryExpecting[List[Record]](using = query(path))(recordsDecoder))
    _       <- validate(maybeAuthUser, records)
  } yield ()

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._

  private def query(path: Path) = SparqlQuery.of(
    name = "project by id",
    Prefixes.of(rdf -> "rdf", schema -> "schema", renku -> "renku"),
    s"""|SELECT DISTINCT ?projectId ?maybeVisibility (GROUP_CONCAT(?maybeMemberGitLabId; separator=',') AS ?memberGitLabIds)
        |WHERE {
        |  BIND (${ResourceId(path)(renkuBaseUrl).showAs[RdfResource]} AS ?projectId)
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
  ): EitherT[Interpretation, EndpointSecurityException, Unit] = records -> maybeAuthUser match {
    case (Nil, _)                                                                            => rightT(())
    case ((Public, _) :: Nil, _)                                                             => rightT(())
    case ((_, projectMembers) :: Nil, Some(authUser)) if projectMembers contains authUser.id => rightT(())
    case _ => leftT(AuthorizationFailure)
  }
}

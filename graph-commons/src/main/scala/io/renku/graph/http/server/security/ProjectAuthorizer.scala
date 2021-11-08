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
import cats.effect.Async
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Visibility._
import io.renku.graph.model.projects.{Path, Visibility}
import io.renku.graph.model.users.GitLabId
import io.renku.http.server.security.EndpointSecurityException
import io.renku.http.server.security.EndpointSecurityException.AuthorizationFailure
import io.renku.http.server.security.model.AuthUser
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

trait ProjectAuthorizer[F[_]] {
  def authorize(path: projects.Path, maybeAuthUser: Option[AuthUser]): EitherT[F, EndpointSecurityException, Unit]
}

object ProjectAuthorizer {
  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[ProjectAuthorizer[F]] = for {
    config <- RdfStoreConfig[F]()
  } yield new ProjectAuthorizerImpl(config, timeRecorder)
}

class ProjectAuthorizerImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with ProjectAuthorizer[F] {

  override def authorize(path:          projects.Path,
                         maybeAuthUser: Option[AuthUser]
  ): EitherT[F, EndpointSecurityException, Unit] = for {
    records <- EitherT.right(queryExpecting[List[Record]](using = query(path))(recordsDecoder))
    _       <- validate(maybeAuthUser, records)
  } yield ()

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._

  private def query(path: Path) = SparqlQuery.of(
    name = "project by id",
    Prefixes.of(schema -> "schema", renku -> "renku"),
    s"""|SELECT DISTINCT ?projectId ?visibility (GROUP_CONCAT(?maybeMemberGitLabId; separator=',') AS ?memberGitLabIds)
        |WHERE {
        |  ?projectId a schema:Project;
        |             renku:projectPath '$path';
        |             renku:projectVisibility ?visibility.
        |  OPTIONAL {
        |    ?projectId schema:member/schema:sameAs ?sameAsId.
        |    ?sameAsId schema:additionalType 'GitLab';
        |              schema:identifier ?maybeMemberGitLabId.
        |  }
        |}
        |GROUP BY ?projectId ?visibility
        |""".stripMargin
  )

  private type Record = (Visibility, Set[GitLabId])

  private lazy val recordsDecoder: Decoder[List[Record]] = {
    import Decoder._

    val recordDecoder: Decoder[Record] = { cursor =>
      for {
        visibility <- cursor.downField("visibility").downField("value").as[Visibility]
        maybeUserId <- cursor
                         .downField("memberGitLabIds")
                         .downField("value")
                         .as[Option[String]]
                         .map(_.map(_.split(",").toList).getOrElse(List.empty))
                         .flatMap(_.map(GitLabId.parse).sequence.leftMap(ex => DecodingFailure(ex.getMessage, Nil)))
                         .map(_.toSet)
      } yield visibility -> maybeUserId
    }

    _.downField("results").downField("bindings").as(decodeList(recordDecoder))
  }

  private def validate(maybeAuthUser: Option[AuthUser],
                       records:       List[Record]
  ): EitherT[F, EndpointSecurityException, Unit] = records -> maybeAuthUser match {
    case (Nil, _)                                                                            => rightT(())
    case ((Public, _) :: Nil, _)                                                             => rightT(())
    case ((_, projectMembers) :: Nil, Some(authUser)) if projectMembers contains authUser.id => rightT(())
    case _ => leftT(AuthorizationFailure)
  }
}

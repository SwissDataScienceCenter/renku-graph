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

package io.renku.graph.http.server.security

import cats.effect.Async
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.http.server.security.Authorizer.{SecurityRecord, SecurityRecordFinder}
import io.renku.graph.model.{projects, GraphClass, RenkuUrl}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.{ResourceId, Visibility}
import io.renku.graph.model.projects.Visibility._
import io.renku.graph.model.views.RdfResource
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore._
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

object TSPathRecordsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[SecurityRecordFinder[F, projects.Path]] = for {
    implicit0(renkuUrl: RenkuUrl) <- RenkuUrlLoader[F]()
    storeConfig                   <- ProjectsConnectionConfig[F]()
  } yield new TSPathRecordsFinderImpl[F](storeConfig)
}

private class TSPathRecordsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    storeConfig: ProjectsConnectionConfig
)(implicit renkuUrl: RenkuUrl)
    extends TSClientImpl(storeConfig)
    with SecurityRecordFinder[F, projects.Path] {

  override def apply(path: projects.Path, maybeAuthUser: Option[AuthUser]): F[List[SecurityRecord]] =
    queryExpecting[List[SecurityRecord]](query(ResourceId(path)))(recordsDecoder(path))

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._

  private def query(resourceId: projects.ResourceId) = SparqlQuery.of(
    name = "authorise - project path",
    Prefixes of (renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?projectId ?visibility (GROUP_CONCAT(?maybeMemberGitLabId; separator=',') AS ?memberGitLabIds)
        |FROM <${GraphClass.Persons.id}>
        |FROM ${resourceId.showAs[RdfResource]} {
        |  BIND (${resourceId.showAs[RdfResource]} AS ?resourceId)
        |  ?projectId a schema:Project;
        |             renku:projectVisibility ?visibility.
        |  OPTIONAL {
        |    ?projectId schema:member/schema:sameAs ?sameAsId.
        |    ?sameAsId schema:additionalType '${Person.gitLabSameAsAdditionalType}';
        |              schema:identifier ?maybeMemberGitLabId.
        |  }
        |}
        |GROUP BY ?projectId ?visibility
        |""".stripMargin
  )

  private def recordsDecoder(path: projects.Path): Decoder[List[SecurityRecord]] =
    ResultsDecoder[List, SecurityRecord] { implicit cur =>
      for {
        visibility <- extract[Visibility]("visibility")
        maybeUserId <- extract[Option[String]]("memberGitLabIds")
                         .map(_.map(_.split(",").toList).getOrElse(List.empty))
                         .flatMap(_.map(GitLabId.parse).sequence.leftMap(ex => DecodingFailure(ex.getMessage, Nil)))
                         .map(_.toSet)
      } yield SecurityRecord(visibility, path, maybeUserId)
    }
}

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
import io.renku.graph.http.server.security.Authorizer.{SecurityRecord, SecurityRecordFinder}
import io.renku.graph.model.{datasets, projects, GraphClass}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.Visibility
import io.renku.triplesstore._
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.typelevel.log4cats.Logger

object DatasetIdRecordsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[SecurityRecordFinder[F, datasets.Identifier]] =
    ProjectsConnectionConfig[F]().map(new DatasetIdRecordsFinderImpl(_))
}

private class DatasetIdRecordsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    storeConfig: ProjectsConnectionConfig
) extends TSClientImpl(storeConfig)
    with SecurityRecordFinder[F, datasets.Identifier] {

  override def apply(id: datasets.Identifier): F[List[SecurityRecord]] =
    queryExpecting[List[SecurityRecord]](selectQuery = query(id))(recordsDecoder)

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._

  private def query(id: datasets.Identifier) = SparqlQuery.of(
    name = "authorise - dataset id",
    Prefixes of (renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?projectId ?path ?visibility (GROUP_CONCAT(?maybeMemberGitLabId; separator=',') AS ?memberGitLabIds)
        |WHERE {
        |  GRAPH ?projectGraph {
        |    ?projectId a schema:Project;
        |               renku:hasDataset/schema:identifier '$id';
        |               renku:projectPath ?path;
        |               renku:projectVisibility ?visibility
        |    OPTIONAL {
        |      ?projectId schema:member ?memberId.
        |      GRAPH <${GraphClass.Persons.id}> {
        |        ?memberId schema:sameAs ?sameAsId.
        |        ?sameAsId schema:additionalType '${Person.gitLabSameAsAdditionalType}';
        |                  schema:identifier ?maybeMemberGitLabId
        |      }
        |    }
        |  }
        |}
        |GROUP BY ?projectId ?path ?visibility
        |""".stripMargin
  )

  private lazy val recordsDecoder: Decoder[List[SecurityRecord]] = ResultsDecoder[List, SecurityRecord] {
    implicit cur =>
      import Decoder._
      import io.renku.tinytypes.json.TinyTypeDecoders._

      for {
        visibility <- extract[Visibility]("visibility")
        path       <- extract[projects.Path]("path")
        userIds <- extract[Option[String]]("memberGitLabIds")
                     .map(_.map(_.split(",").toList).getOrElse(List.empty))
                     .flatMap(_.map(GitLabId.parse).sequence.leftMap(ex => DecodingFailure(ex.getMessage, Nil)))
                     .map(_.toSet)
      } yield (visibility, path, userIds)
  }
}

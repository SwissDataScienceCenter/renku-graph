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
import cats.MonadThrow
import io.renku.graph.http.server.security.Authorizer.{SecurityRecord, SecurityRecordFinder}
import io.renku.graph.model.{datasets, GraphClass}
import io.renku.graph.model.entities.Person
import io.renku.jsonld.EntityId
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

object DatasetSameAsRecordsFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[SecurityRecordFinder[F, datasets.SameAs]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new DatasetSameAsRecordsFinderImpl(_))
}

private class DatasetSameAsRecordsFinderImpl[F[_]: MonadThrow](tsClient: TSClient[F])
    extends SecurityRecordFinder[F, datasets.SameAs] {

  override def apply(sameAs: datasets.SameAs): F[List[SecurityRecord]] =
    tsClient.queryExpecting[List[SecurityRecord]](selectQuery = query(sameAs))

  import eu.timepit.refined.auto._
  import io.circe.{Decoder, DecodingFailure}
  import io.renku.graph.model.{persons, projects}
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.{ResultsDecoder, SparqlQuery}
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import ResultsDecoder._

  private lazy val rowsSeparator = '\u0000'

  private def query(sameAs: datasets.SameAs) = SparqlQuery.of(
    name = "authorise - dataset sameAs",
    Prefixes of (renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?projectId ?path ?visibility (GROUP_CONCAT(?maybeMemberGitLabId; separator='$rowsSeparator') AS ?memberGitLabIds)
        |WHERE {
        |  GRAPH ?projectId {
        |    ?projectId a schema:Project;
        |               renku:hasDataset/renku:topmostSameAs ${EntityId.of(sameAs.value).sparql};
        |               renku:projectPath ?path;
        |               renku:projectVisibility ?visibility.
        |    OPTIONAL {
        |      ?projectId schema:member ?memberId.
        |      GRAPH ${GraphClass.Persons.id.sparql} {
        |        ?memberId schema:sameAs ?sameAsId.
        |        ?sameAsId schema:additionalType ${Person.gitLabSameAsAdditionalType.asTripleObject.asSparql.sparql};
        |                  schema:identifier ?maybeMemberGitLabId
        |      }
        |    }
        |  }
        |}
        |GROUP BY ?projectId ?path ?visibility
        |""".stripMargin
  )

  private implicit lazy val recordsDecoder: Decoder[List[SecurityRecord]] = ResultsDecoder[List, SecurityRecord] {
    implicit cur =>
      import Decoder._
      import io.renku.tinytypes.json.TinyTypeDecoders._

      val toSetOfGitLabIds: Option[String] => Result[Set[persons.GitLabId]] =
        _.map(_.split(rowsSeparator).toList)
          .getOrElse(List.empty)
          .map(persons.GitLabId.parse)
          .sequence
          .bimap(ex => DecodingFailure(ex.getMessage, Nil), _.toSet)

      for {
        visibility <- extract[projects.Visibility]("visibility")
        path       <- extract[projects.Path]("path")
        userIds    <- extract[Option[String]]("memberGitLabIds") >>= toSetOfGitLabIds
      } yield (visibility, path, userIds)
  }
}

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

package io.renku.knowledgegraph.users.projects
package finder

import Endpoint.Criteria
import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model.entities.Person
import io.renku.graph.model.projects
import io.renku.knowledgegraph.users.projects.model.Project
import io.renku.triplesstore.{RenkuConnectionConfig, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait TSProjectFinder[F[_]] {
  def findProjectsInTS(criteria: Criteria): F[List[model.Project.Activated]]
}

private object TSProjectFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[TSProjectFinder[F]] =
    RenkuConnectionConfig[F]().map(new TSProjectFinderImpl(_))
}

private class TSProjectFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    renkuConnectionConfig: RenkuConnectionConfig
) extends TSClient(renkuConnectionConfig)
    with TSProjectFinder[F] {

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes

  override def findProjectsInTS(criteria: Criteria): F[List[Project.Activated]] =
    queryExpecting[List[Project.Activated]](selectQuery = query(criteria))

  private def query(criteria: Criteria) = SparqlQuery.of(
    name = "user projects by id",
    Prefixes.of(schema -> "schema", prov -> "prov", renku -> "renku"),
    s"""|SELECT ?name ?path ?visibility ?dateCreated ?maybeCreatorName 
        |       (GROUP_CONCAT(?keyword; separator=',') AS ?keywords)
        |       ?maybeDescription 
        |WHERE {
        |  ?resourceId a schema:Project;
        |              renku:projectPath ?path;
        |              schema:name ?name;
        |              renku:projectVisibility ?visibility;
        |              schema:dateCreated ?dateCreated.
        |  ${criteria.maybeOnAccessRights("?resourceId", "?visibility")}
        |  ?resourceId schema:member/schema:sameAs ?memberSameAs.
        |  ?memberSameAs schema:additionalType '${Person.gitLabSameAsAdditionalType}';
        |                schema:identifier ${criteria.userId.value}.
        |  OPTIONAL { ?resourceId schema:description ?maybeDescription }
        |  OPTIONAL { ?resourceId schema:keywords ?keyword }
        |  OPTIONAL {
        |    ?resourceId schema:creator ?maybeCreatorResourceId.
        |    ?maybeCreatorResourceId a schema:Person;
        |                            schema:name ?maybeCreatorName.
        |  }
        |}
        |GROUP BY ?resourceId ?name ?path ?visibility ?dateCreated ?maybeCreatorName ?maybeDescription
        |""".stripMargin
  )

  private implicit class CriteriaOps(criteria: Criteria) {

    def maybeOnAccessRights(projectIdVariable: String, visibilityVariable: String): String =
      criteria.maybeUser match {
        case Some(user) =>
          s"""|OPTIONAL {
              |    $projectIdVariable schema:member/schema:sameAs ?memberId.
              |    ?memberId schema:additionalType 'GitLab';
              |              schema:identifier ?userGitlabId .
              |}
              |FILTER (
              |  $visibilityVariable != '${projects.Visibility.Private.value}' || ?userGitlabId = ${user.id.value}
              |)
              |""".stripMargin
        case _ =>
          s"""FILTER ($visibilityVariable = '${projects.Visibility.Public.value}')"""
      }
  }

  private implicit lazy val recordsDecoder: Decoder[List[Project.Activated]] = {
    import Decoder._
    import io.circe.DecodingFailure
    import io.renku.graph.model.persons
    import io.renku.graph.model.projects._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val toSetOfKeywords: Option[String] => Decoder.Result[Set[Keyword]] =
      _.map(_.split(',').toList.map(Keyword.from).sequence.map(_.toSet)).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(Set.empty))

    ResultsDecoder[List, Project.Activated] { implicit cur =>
      for {
        name         <- extract[Name]("name")
        path         <- extract[Path]("path")
        visibility   <- extract[Visibility]("visibility")
        dateCreated  <- extract[DateCreated]("dateCreated")
        maybeCreator <- extract[Option[persons.Name]]("maybeCreatorName")
        keywords     <- extract[Option[String]]("keywords").flatMap(toSetOfKeywords)
        maybeDesc    <- extract[Option[Description]]("maybeDescription")
      } yield Project.Activated(name, path, visibility, dateCreated, maybeCreator, keywords.toList.sorted, maybeDesc)
    }
  }
}

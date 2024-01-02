/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model.GraphClass
import io.renku.graph.model.projects.Visibility
import io.renku.knowledgegraph.users.projects.Endpoint.Criteria
import io.renku.knowledgegraph.users.projects.model.Project
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesstore._
import io.renku.triplesstore.client.sparql.{Fragment, VarName}
import io.renku.triplesstore.client.syntax._
import org.typelevel.log4cats.Logger

private trait TSProjectFinder[F[_]] {
  def findProjectsInTS(criteria: Criteria): F[List[model.Project.Activated]]
}

private object TSProjectFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[TSProjectFinder[F]] =
    ProjectsConnectionConfig[F]().map(new TSProjectFinderImpl(_))
}

private class TSProjectFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClientImpl(storeConfig)
    with TSProjectFinder[F] {

  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.SparqlQuery.Prefixes

  override def findProjectsInTS(criteria: Criteria): F[List[Project.Activated]] =
    queryExpecting[List[Project.Activated]](query(criteria))

  private val authSnippets = SparqlSnippets(VarName("projectId"))

  private def query(criteria: Criteria) = SparqlQuery.of(
    name = "user projects by id",
    Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
    sparql"""|SELECT ?name ?slug ?visibility ?dateCreated ?maybeCreatorName
             |       (GROUP_CONCAT(?keyword; separator=',') AS ?keywords)
             |       ?maybeDescription 
             |WHERE {
             |
             |  ${memberProjects(criteria)}
             |
             |  ${maybeOnAccessRights(criteria)}
             |
             |  GRAPH ?projectId {
             |    ?projectId 
             |               a schema:Project;
             |               renku:projectPath ?slug;
             |               schema:name ?name;
             |               renku:projectVisibility ?visibility;
             |               schema:dateCreated ?dateCreated.
             |
             |    OPTIONAL { ?projectId schema:description ?maybeDescription }
             |    OPTIONAL { ?projectId schema:keywords ?keyword }
             |    OPTIONAL {
             |      ?projectId schema:creator ?maybeCreatorResourceId.
             |      GRAPH ${GraphClass.Persons.id} {
             |        ?maybeCreatorResourceId schema:name ?maybeCreatorName
             |      }
             |    }
             |  }
             |}
             |GROUP BY ?projectId ?name ?slug ?visibility ?dateCreated ?maybeCreatorName ?maybeDescription
             |""".stripMargin
  )

  private def memberProjects(criteria: Criteria): Fragment =
    authSnippets.memberProjects(criteria.userId)

  private def maybeOnAccessRights(criteria: Criteria): Fragment =
    authSnippets
      .visibleProjects(criteria.maybeUser.map(_.id), Visibility.all)

  private implicit lazy val recordsDecoder: Decoder[List[Project.Activated]] = {
    import Decoder._
    import ResultsDecoder._
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
        slug         <- extract[Slug]("slug")
        visibility   <- extract[Visibility]("visibility")
        dateCreated  <- extract[DateCreated]("dateCreated")
        maybeCreator <- extract[Option[persons.Name]]("maybeCreatorName")
        keywords     <- extract[Option[String]]("keywords").flatMap(toSetOfKeywords)
        maybeDesc    <- extract[Option[Description]]("maybeDescription")
      } yield Project.Activated(name, slug, visibility, dateCreated, maybeCreator, keywords.toList.sorted, maybeDesc)
    }
  }
}

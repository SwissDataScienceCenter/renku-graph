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

package io.renku.knowledgegraph.users.projects
package finder

import Endpoint.Criteria
import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model.{projects, GraphClass}
import io.renku.graph.model.entities.Person
import io.renku.knowledgegraph.users.projects.model.Project
import io.renku.triplesstore._
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

  private def query(criteria: Criteria) = SparqlQuery.of(
    name = "user projects by id",
    Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
    s"""|SELECT ?name ?path ?visibility ?dateCreated ?maybeCreatorName 
        |       (GROUP_CONCAT(?keyword; separator=',') AS ?keywords)
        |       ?maybeDescription 
        |WHERE {
        |  GRAPH ${GraphClass.Persons.id.asSparql.sparql} {
        |    ?memberSameAs schema:additionalType ${Person.gitLabSameAsAdditionalType.asTripleObject.asSparql.sparql};
        |                  schema:identifier ${criteria.userId.asObject.asSparql.sparql};
        |                  ^schema:sameAs ?memberId
        |  }
        |  GRAPH ?projectId {
        |    ?projectId schema:member ?memberId;
        |               a schema:Project;
        |               renku:projectPath ?path;
        |               schema:name ?name;
        |               renku:projectVisibility ?visibility;
        |               schema:dateCreated ?dateCreated.
        |    ${criteria.maybeOnAccessRights("?projectId", "?visibility")}
        |    OPTIONAL { ?projectId schema:description ?maybeDescription }
        |    OPTIONAL { ?projectId schema:keywords ?keyword }
        |    OPTIONAL {
        |      ?projectId schema:creator ?maybeCreatorResourceId.
        |      GRAPH ${GraphClass.Persons.id.asSparql.sparql} {
        |        ?maybeCreatorResourceId schema:name ?maybeCreatorName
        |      }
        |    }
        |  }
        |}
        |GROUP BY ?projectId ?name ?path ?visibility ?dateCreated ?maybeCreatorName ?maybeDescription
        |""".stripMargin
  )

  private implicit class CriteriaOps(criteria: Criteria) {

    def maybeOnAccessRights(projectIdVariable: String, visibilityVariable: String): String =
      criteria.maybeUser match {
        case Some(user) =>
          s"""|{
              |  VALUES ($visibilityVariable) {
              |    (${projects.Visibility.Public.asObject.asSparql.sparql})
              |    (${projects.Visibility.Internal.asObject.asSparql.sparql})
              |  }
              |} UNION {
              |  VALUES ($visibilityVariable) {
              |    (${projects.Visibility.Private.asObject.asSparql.sparql})
              |  }
              |  $projectIdVariable schema:member ?projectMemberId
              |  GRAPH ${GraphClass.Persons.id.asSparql.sparql} {
              |    ?projectMemberId schema:sameAs ?projectMemberSameAs.
              |    ?projectMemberSameAs schema:additionalType ${Person.gitLabSameAsAdditionalType.asTripleObject.asSparql.sparql};
              |                         schema:identifier ${user.id.asObject.asSparql.sparql}
              |  }
              |}
              |""".stripMargin
        case _ =>
          s"""|VALUES ($visibilityVariable) {
              |  (${projects.Visibility.Public.asObject.asSparql.sparql})
              |}""".stripMargin
      }
  }

  private implicit lazy val recordsDecoder: Decoder[List[Project.Activated]] = {
    import Decoder._
    import io.circe.DecodingFailure
    import io.renku.graph.model.persons
    import io.renku.graph.model.projects._
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import ResultsDecoder._

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

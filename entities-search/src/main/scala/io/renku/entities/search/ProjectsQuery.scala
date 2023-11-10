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

package io.renku.entities.search

import io.circe.Decoder
import io.renku.entities.search.Criteria.Filters.EntityType
import io.renku.entities.search.model.{Entity, MatchingScore}
import io.renku.entities.searchgraphs.concatSeparator
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.http.server.security.model.AuthUser
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import io.renku.triplesstore.client.syntax._

private case object ProjectsQuery extends EntityQuery[model.Entity.Project] {

  override val entityType: EntityType = EntityType.Project

  private val matchingScoreVar    = VarName("matchingScore")
  private val nameVar             = VarName("name")
  private val slugVar             = VarName("slug")
  private val visibilityVar       = VarName("visibility")
  private val dateVar             = VarName("date")
  private val dateModifiedVar     = VarName("dateModified")
  private val maybeDateCreatedVar = VarName("maybeDateCreated")
  private val maybeCreatorNameVar = VarName("maybeCreatorName")
  private val maybeDescriptionVar = VarName("maybeDescription")
  private val keywordsVar         = VarName("keywords")
  private val imagesVar           = VarName("images")

  // local vars
  private val projectIdVar = VarName("projectId")
  private val authSnippets = SparqlSnippets(projectIdVar)

  override val selectVariables: Set[String] =
    Set(
      entityTypeVar,
      matchingScoreVar,
      nameVar,
      slugVar,
      visibilityVar,
      dateVar,
      dateModifiedVar,
      maybeCreatorNameVar,
      maybeDescriptionVar,
      keywordsVar,
      imagesVar
    ).map(_.name)

  override def query(criteria: Criteria): Option[Fragment] = (criteria.filters whenRequesting entityType) {
    import criteria._
    sparql"""|{
             |  SELECT DISTINCT $entityTypeVar $matchingScoreVar $nameVar $slugVar $visibilityVar
             |    $maybeDateCreatedVar $dateModifiedVar ($dateModifiedVar AS $dateVar)
             |    $maybeCreatorNameVar
             |    $maybeDescriptionVar
             |    $keywordsVar
             |    $imagesVar
             |  WHERE {
             |    BIND (${entityType.value.asTripleObject} AS $entityTypeVar)
             |
             |    # textQuery
             |    ${textQueryPart(criteria.filters.maybeQuery)}
             |
             |    GRAPH ${GraphClass.Projects.id} {
             |      $projectIdVar a renku:DiscoverableProject;
             |                    schema:name $nameVar;
             |                    renku:slug $slugVar;
             |                    schema:dateModified $dateModifiedVar;
             |                    schema:dateCreated $maybeDateCreatedVar.
             |
             |      ${filters.maybeOnDateCreated(maybeDateCreatedVar)}
             |
             |      ${accessRightsAndVisibility(criteria.maybeUser, criteria.filters)}
             |
             |      GRAPH $projectIdVar {
             |        ${namespacesPart(criteria.filters.namespaces)}
             |      }
             |
             |      OPTIONAL {
             |        $projectIdVar schema:creator ?creatorId.
             |        GRAPH ${GraphClass.Persons.id} {
             |          ?creatorId schema:name $maybeCreatorNameVar
             |        }
             |      }
             |      ${filters.maybeOnCreatorName(maybeCreatorNameVar)}
             |
             |      OPTIONAL { $projectIdVar schema:description $maybeDescriptionVar }
             |      OPTIONAL { $projectIdVar renku:keywordsConcat $keywordsVar }
             |      OPTIONAL { $projectIdVar renku:imagesConcat $imagesVar }
             |    }
             |  }
             |}
             |""".stripMargin
  }

  private def textQueryPart: Option[Criteria.Filters.Query] => Fragment = {
    case Some(q) =>
      val luceneQuery = LuceneQuery.fuzzy(q.value)
      fr"""|{
           |  SELECT $projectIdVar (MAX(?score) AS $matchingScoreVar)
           |  WHERE {
           |    {
           |      (?id ?score) text:query (schema:name renku:keywordsConcat schema:description renku:projectNamespaces $luceneQuery)
           |    } {
           |      GRAPH ${GraphClass.Projects.id} {
           |        ?id a renku:DiscoverableProject
           |      }
           |      BIND (?id AS $projectIdVar)
           |    } UNION {
           |      GRAPH ${GraphClass.Projects.id} {
           |        $projectIdVar schema:creator ?id;
           |                      a renku:DiscoverableProject
           |      }
           |    }
           |  }
           |  GROUP BY $projectIdVar
           |}
           |""".stripMargin
    case None =>
      fr"""|BIND (xsd:float(1.0) AS $matchingScoreVar)
           |""".stripMargin
  }

  private def accessRightsAndVisibility(maybeUser: Option[AuthUser], filters: Criteria.Filters): Fragment =
    maybeUser.map(_.id) -> filters.roles match {
      case Some(userId) -> roles if roles.nonEmpty =>
        fr"""|$projectIdVar renku:projectVisibility $visibilityVar .
             |${visibilitiesPart(filters.visibilities)}
             |${authSnippets.projectsWithRoles(userId, roles)}
             |""".stripMargin
      case maybeUserId -> _ =>
        fr"""|$projectIdVar renku:projectVisibility $visibilityVar .
             |${authSnippets.visibleProjects(maybeUserId, filters.visibilities)}
             |""".stripMargin
    }

  private lazy val visibilitiesPart: Set[projects.Visibility] => Fragment = {
    case vis if vis.nonEmpty => fr"""VALUES ($visibilityVar) {${vis.map(_.value)}}."""
    case _                   => Fragment.empty
  }

  private def namespacesPart(ns: Set[projects.Namespace]): Fragment = {
    val matchFrag =
      if (ns.isEmpty) Fragment.empty
      else fr"VALUES (?namespace) { ${ns.map(_.value)} }"

    fr"""|$projectIdVar renku:projectNamespace ?namespace.
         |$matchFrag
    """.stripMargin
  }

  override def decoder[EE >: Entity.Project]: Decoder[EE] = { implicit cursor =>
    import DecodingTools._
    import cats.syntax.all._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    for {
      matchingScore    <- read[MatchingScore](matchingScoreVar)
      slug             <- read[projects.Slug](slugVar)
      name             <- read[projects.Name](nameVar)
      visibility       <- read[projects.Visibility](visibilityVar)
      dateCreated      <- read[projects.DateCreated](maybeDateCreatedVar)
      dateModified     <- read[projects.DateModified](dateModifiedVar)
      maybeCreatorName <- read[Option[persons.Name]](maybeCreatorNameVar)
      maybeDescription <- read[Option[projects.Description]](maybeDescriptionVar)
      images           <- read[Option[String]](imagesVar) >>= toListOfImageUris(concatSeparator)
      keywords <-
        read[Option[String]](keywordsVar) >>= toListOf[projects.Keyword, projects.Keyword.type](concatSeparator)
    } yield Entity.Project(matchingScore,
                           slug,
                           name,
                           visibility,
                           dateCreated,
                           dateModified,
                           maybeCreatorName,
                           keywords,
                           maybeDescription,
                           images
    )
  }
}

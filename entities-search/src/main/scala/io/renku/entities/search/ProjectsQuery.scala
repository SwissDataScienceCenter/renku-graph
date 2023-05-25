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
import io.renku.graph.model.entities.Person
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import io.renku.triplesstore.client.syntax._

private case object ProjectsQuery extends EntityQuery[model.Entity.Project] {

  override val entityType: EntityType = EntityType.Project

  private val matchingScoreVar    = VarName("matchingScore")
  private val nameVar             = VarName("name")
  private val pathVar             = VarName("path")
  private val visibilityVar       = VarName("visibility")
  private val dateVar             = VarName("date")
  private val maybeCreatorNameVar = VarName("maybeCreatorName")
  private val maybeDescriptionVar = VarName("maybeDescription")
  private val keywordsVar         = VarName("keywords")
  private val imagesVar           = VarName("images")

  // local vars
  private val projectIdVar       = VarName("projectId")
  private val keywordVar         = VarName("keyword")
  private val encodedImageUrlVar = VarName("encodedImageUrl")

  override val selectVariables: Set[String] = Set(entityTypeVar,
                                                  matchingScoreVar,
                                                  nameVar,
                                                  pathVar,
                                                  visibilityVar,
                                                  dateVar,
                                                  maybeCreatorNameVar,
                                                  maybeDescriptionVar,
                                                  keywordsVar,
                                                  imagesVar
  ).map(_.name)

  override def query(criteria: Criteria): Option[String] = (criteria.filters whenRequesting entityType) {
    import criteria._
    sparql"""|{
             |  SELECT $entityTypeVar $matchingScoreVar $nameVar $pathVar $visibilityVar $dateVar $maybeCreatorNameVar
             |    $maybeDescriptionVar (GROUP_CONCAT(DISTINCT $keywordVar; separator=',') AS $keywordsVar)
             |    (GROUP_CONCAT($encodedImageUrlVar; separator=',') AS $imagesVar)
             |  WHERE {
             |    BIND ('project' AS $entityTypeVar)
             |
             |    # textQuery
             |    ${textQueryPart(criteria.filters.maybeQuery)}
             |
             |    GRAPH ${GraphClass.Projects.id} {
             |      $projectIdVar a renku:DiscoverableProject;
             |                    schema:name $nameVar;
             |                    renku:projectPath $pathVar;
             |                    schema:dateCreated $dateVar.
             |
             |      GRAPH $projectIdVar {
             |        ${accessRightsAndVisibility(criteria.maybeUser, criteria.filters.visibilities)}
             |        ${namespacesPart(criteria.filters.namespaces)}
             |      }
             |      
             |      ${filters.maybeOnDateCreated(dateVar)}
             |      ${filters.maybeOnCreatorName(maybeCreatorNameVar)}
             |
             |      OPTIONAL { $projectIdVar schema:creator/schema:name $maybeCreatorNameVar }
             |      OPTIONAL { $projectIdVar schema:description $maybeDescriptionVar }
             |      OPTIONAL { $projectIdVar schema:keywords $keywordVar }
             |      $imagesPart
             |    }
             |  }
             |  GROUP BY $entityTypeVar $matchingScoreVar $nameVar $pathVar
             |    $visibilityVar $dateVar $maybeCreatorNameVar $maybeDescriptionVar
             |}
             |""".stripMargin.sparql
  }

  private def textQueryPart: Option[Criteria.Filters.Query] => Fragment = {
    case Some(q) =>
      val luceneQuery = LuceneQuery.fuzzy(q.value)
      fr"""|{
           |  SELECT $projectIdVar (MAX(?score) AS $matchingScoreVar)
           |  WHERE {
           |    {
           |      (?id ?score) text:query (schema:name schema:keywords schema:description renku:projectNamespaces $luceneQuery)
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

  private def accessRightsAndVisibility(maybeUser: Option[AuthUser], visibilities: Set[projects.Visibility]): Fragment =
    maybeUser match {
      case Some(user) =>
        val nonPrivateVisibilities =
          if (visibilities.isEmpty)
            projects.Visibility.all - projects.Visibility.Private
          else (projects.Visibility.all - projects.Visibility.Private) intersect visibilities

        val selectPrivateValue =
          if (visibilities.isEmpty || visibilities.contains(projects.Visibility.Private))
            projects.Visibility.Private.asObject
          else "".asTripleObject
        fr"""|{
             |  $projectIdVar renku:projectVisibility $visibilityVar.
             |  {
             |    VALUES ($visibilityVar) {
             |      ${nonPrivateVisibilities.map(_.asObject)}
             |    }
             |  } UNION {
             |    VALUES ($visibilityVar) {
             |      ($selectPrivateValue)
             |    }
             |    $projectIdVar schema:member ?memberId.
             |    GRAPH ${GraphClass.Persons.id} {
             |      ?memberId schema:sameAs ?memberSameAs.
             |      ?memberSameAs schema:additionalType ${Person.gitLabSameAsAdditionalType.asTripleObject};
             |                    schema:identifier ${user.id.asObject}
             |    }
             |  }
             |}
             |""".stripMargin
      case None =>
        visibilities match {
          case v if v.isEmpty || v.contains(projects.Visibility.Public) =>
            fr"""|$projectIdVar renku:projectVisibility $visibilityVar.
                 |VALUES ($visibilityVar) { (${projects.Visibility.Public.asObject}) }""".stripMargin
          case _ =>
            fr"""|$projectIdVar renku:projectVisibility $visibilityVar.
                 |VALUES ($visibilityVar) { ('') }""".stripMargin
        }
    }

  private def namespacesPart(ns: Set[projects.Namespace]): Fragment = {
    val matchFrag =
      if (ns.isEmpty) Fragment.empty
      else fr"VALUES (?namespace) { ${ns.map(_.value)} }"

    fr"""|$projectIdVar renku:projectNamespace ?namespace.
         |$matchFrag
    """.stripMargin
  }

  private lazy val imagesPart =
    fr"""|OPTIONAL {
         |  $projectIdVar schema:image ?imageId.
         |  ?imageId schema:position ?imagePosition;
         |           schema:contentUrl ?imageUrl.
         |  BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS $encodedImageUrlVar)
         |}
         |""".stripMargin

  override def decoder[EE >: Entity.Project]: Decoder[EE] = { implicit cursor =>
    import DecodingTools._
    import cats.syntax.all._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    for {
      matchingScore    <- read[MatchingScore](matchingScoreVar)
      path             <- read[projects.Path](pathVar)
      name             <- read[projects.Name](nameVar)
      visibility       <- read[projects.Visibility](visibilityVar)
      dateCreated      <- read[projects.DateCreated](dateVar)
      maybeCreatorName <- read[Option[persons.Name]](maybeCreatorNameVar)
      maybeDescription <- read[Option[projects.Description]](maybeDescriptionVar)
      images           <- read[Option[String]](imagesVar) >>= toListOfImageUris
      keywords <-
        read[Option[String]](keywordsVar) >>= toListOf[projects.Keyword, projects.Keyword.type](projects.Keyword)
    } yield Entity.Project(matchingScore,
                           path,
                           name,
                           visibility,
                           dateCreated,
                           maybeCreatorName,
                           keywords,
                           maybeDescription,
                           images
    )
  }
}

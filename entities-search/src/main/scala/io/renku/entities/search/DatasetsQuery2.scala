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

import cats.syntax.all._
import io.circe.Decoder
import io.renku.entities.search.Criteria.Filters
import io.renku.graph.model.entities.Person
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import model.Entity

object DatasetsQuery2 extends EntityQuery[Entity.Dataset] {
  override val entityType: Filters.EntityType = Filters.EntityType.Dataset

  val entityTypeVar           = VarName("entityType")
  val matchingScoreVar        = VarName("matchingScore")
  val nameVar                 = VarName("name")
  val idsPathsVisibilitiesVar = VarName("idsPathsVisibilities")
  val sameAsVar               = VarName("sameAs")
  val maybeDateCreatedVar     = VarName("maybeDateCreated")
  val maybeDatePublishedVar   = VarName("maybeDatePublished")
  val dateVar                 = VarName("date")
  val creatorsNamesVar        = VarName("creatorsNames")
  val maybeDescriptionVar     = VarName("maybeDescription")
  val keywordsVar             = VarName("keywords")
  val imagesVar               = VarName("images")
  val dsId                    = VarName("id")

  override val selectVariables = Set(
    entityTypeVar,
    matchingScoreVar,
    nameVar,
    idsPathsVisibilitiesVar,
    sameAsVar,
    maybeDateCreatedVar,
    maybeDatePublishedVar,
    dateVar,
    creatorsNamesVar,
    maybeDescriptionVar,
    keywordsVar,
    imagesVar
  ).map(_.name)

  //TODO reorganize fragments to go into one graph selection
  override def query(criteria: Criteria): Option[String] =
    criteria.filters.whenRequesting(entityType) {
      fr"""SELECT $entityTypeVar
          |       $matchingScoreVar
          |       $nameVar
          |       $idsPathsVisibilitiesVar
          |       $sameAsVar
          |       $maybeDateCreatedVar
          |       $maybeDatePublishedVar
          |       $dateVar
          |       $creatorsNamesVar
          |       $maybeDescriptionVar
          |       $keywordsVar
          |       $imagesVar
          |WHERE {
          |  BIND ('dataset' AS $entityTypeVar)
          |  # textQuery
          |  ${textQueryPart(criteria.filters.maybeQuery)}
          |
          |  BIND($dsId AS $sameAsVar)
          |
          | { # start sub select
          |  SELECT $dsId (SAMPLE(?projId) as ?projectId)
          |    (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS $creatorsNamesVar)
          |    (GROUP_CONCAT(DISTINCT ?idPathVisibility; separator=',') AS ?idsPathsVisibilities)
          |    (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS $keywordsVar)
          |    (GROUP_CONCAT(?encodedImageUrl; separator=',') AS $imagesVar)
          |  WHERE {
          |    # creators
          |    ${creatorsPart(criteria.filters.creators)}
          |
          |    # dates, keywords, description
          |    ${datesPart(criteria.filters.maybeSince, criteria.filters.maybeUntil)}
          |
          |    # images
          |    
          |
          |    # resolve project
          |    $resolveProject
          |
          |    # project namespaces
          |    ${namespacesPart(criteria.filters.namespaces)}
          |
          |  }
          |  GROUP BY $dsId
          |  } # end sub select
          |
          |}
          |""".stripMargin.sparql
    }

  override def decoder[EE >: Entity.Dataset]: Decoder[EE] = DatasetsQuery.decoder

  def pathVisibility: Fragment =
//    |${
//      criteria.maybeOnAccessRightsAndVisibility("?projectId", "?visibility")
//    }
//    | BIND (CONCAT(STR(? identifier), STR(':'), STR(? projectPath), STR(':'), STR(? visibility)) AS ? idPathVisibility)

    Fragment.empty

  def accessRightsAndVisibility(maybeUser: Option[AuthUser], visibilities: Set[Visibility]): Fragment =
    maybeUser match {
      case Some(user) =>
        val nonPrivateVisibilities =
          if (visibilities.isEmpty)
            projects.Visibility.all - projects.Visibility.Private
          else (projects.Visibility.all - projects.Visibility.Private) intersect visibilities

        val selectPrivateValue =
          if (visibilities.isEmpty || visibilities.contains(projects.Visibility.Private))
            projects.Visibility.Private.asObject.asSparql.sparql
          else "''"
        fr"""|{
             |  ?projectId renku:projectVisibility ?visibility .
             |  {
             |    VALUES (?visibility) {
             |      ${nonPrivateVisibilities.map(_.asObject)}
             |    }
             |  } UNION {
             |    VALUES (?visibility) {
             |      ($selectPrivateValue)
             |    }
             |    ?projectId schema:member ?memberId.
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
            fr"""|?projectId renku:projectVisibility ?visibility .
                 |VALUES (?visibility) { (${projects.Visibility.Public.asObject}) }""".stripMargin
          case _ =>
            fr"""|?projectId renku:projectVisibility ?visibility .
                 |VALUES (?visibility) { ('') }""".stripMargin
        }
    }

  def imagesPart: Fragment =
    fr"""
        |  OPTIONAL {
        |    $dsId schema:image ?imageId .
        |    ?imageId schema:position ?imagePosition ;
        |             schema:contentUrl ?imageUrl .
        |    BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
        |  }
        |""".stripMargin

  def resolveProject: Fragment =
    fr"""
        |  Graph schema:Dataset {
        |    $dsId a renku:DiscoverableDataset;
        |            renku:datasetProjectLink / renku:project ?projId.
        |  }
        |""".stripMargin

  def datesPart(maybeSince: Option[Filters.Since], maybeUntil: Option[Filters.Until]): Fragment = {
    def dateCond(varName: VarName) = {
      val cond =
        List(
          maybeSince.map(s => fr"$varName >= ${s.value}"),
          maybeUntil.map(s => fr"$varName <= ${s.value}")
        ).flatten.intercalate(fr" && ")

      if (cond.isEmpty) Fragment.empty
      else fr"FILTER ($cond)"
    }

    fr"""
        |  Graph schema:Dataset {
        |    $dsId renku:slug $nameVar
        |    OPTIONAL {
        |      $dsId schema:dateCreated $maybeDateCreatedVar.
        |      BIND ($maybeDateCreatedVar AS $dateVar)
        |      ${dateCond(maybeDateCreatedVar)}
        |    }
        |    OPTIONAL {
        |      $dsId schema:datePublished $maybeDatePublishedVar
        |      BIND ($maybeDatePublishedVar AS $dateVar)
        |      ${dateCond(maybeDatePublishedVar)}
        |    }
        |    OPTIONAL {
        |      $dsId schema:keywords ?keyword
        |    }
        |    OPTIONAL {
        |      $dsId schema:description $maybeDescriptionVar
        |    }
        |  }
        |
        |""".stripMargin
  }

  def namespacesPart(ns: Set[projects.Namespace]): Fragment = {
    val matchFrag =
      if (ns.isEmpty) Fragment.empty
      else fr"Values (?namespace) { ${ns.map(_.value)}  }"

    fr"""
        | GRAPH ?projId {
        |  ?projId renku:projectPath ?projectPath;
        |          renku:projectNamespace ?namespace.
        |  $matchFrag
        |
        |        ?projId renku:projectVisibility ?visibility .
        |        VALUES (?visibility) { ('public') }
        |
        |        # Return all visibilities and select the broadest in decoding
        |        BIND (CONCAT(STR(?projectPath), STR(':'),
        |                     STR(?visibility)) AS ?idPathVisibility)
        |
        |}
      """.stripMargin
  }

  def creatorsPart(creators: Set[persons.Name]): Fragment = {
    val matchFrag =
      if (creators.isEmpty) Fragment.empty
      else fr"Values (?creatorName) { ${creators.map(_.value)} }"

    fr"""
        |  Graph schema:Dataset {
        |    OPTIONAL {
        |      $dsId schema:creator / schema:name ?creatorName.
        |      $matchFrag
        |    }
        |  }
        |
        |""".stripMargin
  }

  def textQueryPart(mq: Option[Filters.Query]): Fragment =
    mq match {
      case Some(q) =>
        val luceneQuery = LuceneQuery(q.value)
        fr"""
            |{
            |  SELECT $dsId (MAX(?score) AS $matchingScoreVar)
            |  WHERE {
            |    Graph schema:Dataset {
            |      ($dsId ?score) text:query (renku:slug schema:keywords schema:description schema:name $luceneQuery).
            |    }
            |  }
            |  group by $dsId
            |}
            |
            |""".stripMargin

      case None =>
        fr"""
            |  Bind (xsd:float(1.0) as $matchingScoreVar)
            |  Graph schema:Dataset {
            |    $dsId a renku:DiscoverableDataset.
            |  }
            """.stripMargin
    }
}

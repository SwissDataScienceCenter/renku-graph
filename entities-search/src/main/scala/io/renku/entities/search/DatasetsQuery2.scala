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
import io.renku.graph.model.{GraphClass, RenkuUrl, persons, projects}
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import model.Entity

import java.time.ZoneOffset

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

  // TODO reorganize fragments to go into one graph selection
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
          |  { # start sub select
          |    SELECT $sameAsVar (SAMPLE(?projId) as ?projectId) (SAMPLE(?date1) as $dateVar)
          |      (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS $creatorsNamesVar)
          |      (GROUP_CONCAT(DISTINCT ?idPathVisibility; separator=',') AS $idsPathsVisibilitiesVar)
          |      (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS $keywordsVar)
          |      (GROUP_CONCAT(?encodedImageUrl; separator=',') AS $imagesVar)
          |    WHERE {
          |      Graph schema:Dataset {
          |        #creator
          |        ${creatorsPart(criteria.filters.creators)}
          |
          |        # dates, keywords, description
          |        ${datesPart(criteria.filters.maybeSince, criteria.filters.maybeUntil)}
          |
          |        # resolve project
          |        $resolveProject
          |      }
          |
          |      Graph ?projId {
          |        # project namespaces
          |        ${namespacesPart(criteria.filters.namespaces)}
          |
          |        # access restriction
          |        ${accessRightsAndVisibility(criteria.maybeUser, criteria.filters.visibilities)}
          |
          |        # path and visibility
          |        $pathVisibility
          |      }
          |    }
          |    GROUP BY $sameAsVar
          |  }# end sub select
          |
          |  $images
          |}
          |""".stripMargin.sparql
    }

  override def decoder[EE >: Entity.Dataset](implicit renkuUrl: RenkuUrl): Decoder[EE] = DatasetsQuery.decoder

  def pathVisibility: Fragment =
    fr"""|  # Return all visibilities and select the broadest in decoding
         |  BIND (CONCAT(STR(?projectPath), STR(':'),
         |               STR(?visibility)) AS ?idPathVisibility)
         |""".stripMargin

  def accessRightsAndVisibility(maybeUser: Option[AuthUser], visibilities: Set[Visibility]): Fragment =
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
             |  ?projId renku:projectVisibility ?visibility .
             |  {
             |    VALUES (?visibility) {
             |      ${nonPrivateVisibilities.map(_.asObject)}
             |    }
             |  } UNION {
             |    VALUES (?visibility) {
             |      ($selectPrivateValue)
             |    }
             |    ?projId schema:member ?memberId.
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
            fr"""|?projId renku:projectVisibility ?visibility .
                 |VALUES (?visibility) { (${projects.Visibility.Public.asObject}) }""".stripMargin
          case _ =>
            fr"""|?projId renku:projectVisibility ?visibility .
                 |VALUES (?visibility) { ('') }""".stripMargin
        }
    }

  def images: Fragment =
    fr"""|# images
         |  {
         |    SELECT (GROUP_CONCAT(DISTINCT ?encodedImageUrl; separator=',') AS ?images)
         |    WHERE {
         |      Graph schema:Dataset {
         |        OPTIONAL {
         |          ?sameAs schema:image ?imageId .
         |          ?imageId schema:position ?imagePosition ;
         |                   schema:contentUrl ?imageUrl .
         |          BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
         |        }
         |      }
         |    }
         |}
         |""".stripMargin

  def resolveProject: Fragment =
    fr"""|    $sameAsVar a renku:DiscoverableDataset;
         |            renku:datasetProjectLink / renku:project ?projId.
         |""".stripMargin

  def datesPart(maybeSince: Option[Filters.Since], maybeUntil: Option[Filters.Until]): Fragment = {
    val sinceUTC =
      maybeSince.map(_.value.atStartOfDay().atZone(ZoneOffset.UTC)).map(d => fr"xsd:date($d)")
    val untilUTC =
      maybeUntil.map(_.value.atStartOfDay().atZone(ZoneOffset.UTC)).map(d => fr"xsd:date($d)")

    val sinceLocal =
      maybeSince.map(_.value).map(d => fr"xsd:date($d)")
    val untilLocal =
      maybeUntil.map(_.value).map(d => fr"xsd:date($d)")

    def dateCondCreated = {
      val cond =
        List(
          sinceUTC.map(s => fr"$maybeDateCreatedVar >= $s"),
          untilUTC.map(s => fr"$maybeDateCreatedVar <= $s")
        ).flatten.intercalate(fr" && ")

      if (cond.isEmpty) Fragment.empty
      else fr"FILTER ($cond)"
    }

    def dateCondPublished = {
      val cond =
        List(
          sinceLocal.map(s => fr"$maybeDatePublishedVar >= $s"),
          untilLocal.map(s => fr"$maybeDatePublishedVar <= $s")
        ).flatten.intercalate(fr" && ")

      if (cond.isEmpty) Fragment.empty
      else fr"FILTER ($cond)"
    }

    fr"""|    $sameAsVar renku:slug $nameVar
         |    OPTIONAL {
         |      $sameAsVar schema:dateCreated $maybeDateCreatedVar.
         |      BIND ($maybeDateCreatedVar AS ?date1)
         |      $dateCondCreated
         |    }
         |    OPTIONAL {
         |      $sameAsVar schema:datePublished $maybeDatePublishedVar
         |      BIND ($maybeDatePublishedVar AS ?date1)
         |      $dateCondPublished
         |    }
         |    OPTIONAL {
         |      $sameAsVar schema:keywords ?keyword
         |    }
         |    OPTIONAL {
         |      $sameAsVar schema:description $maybeDescriptionVar
         |    }
         |""".stripMargin
  }

  def namespacesPart(ns: Set[projects.Namespace]): Fragment = {
    val matchFrag =
      if (ns.isEmpty) Fragment.empty
      else fr"Values (?namespace) { ${ns.map(_.value)}  }"

    fr"""|  ?projId renku:projectPath ?projectPath;
         |          renku:projectNamespace ?namespace.
         |  $matchFrag
      """.stripMargin
  }

  def creatorsPart(creators: Set[persons.Name]): Fragment = {
    val creatorNames = fr"$sameAsVar schema:creator / schema:name ?creatorName."
    val matchFrag =
      fr"""Bind (LCASE(?creatorName) as ?creatorNameLC)
           Values (?creatorNameLC) { ${creators.map(_.value.toLowerCase)} }
          """

    if (creators.isEmpty) {
      fr"""|OPTIONAL {
           |  $creatorNames
           |}
           |""".stripMargin
    } else
      fr"""|$creatorNames
           |$matchFrag
           |""".stripMargin
  }

  def textQueryPart(mq: Option[Filters.Query]): Fragment =
    mq match {
      case Some(q) =>
        val luceneQuery = LuceneQuery(q.value)
        fr"""|{
             |  SELECT $sameAsVar (MAX(?score) AS $matchingScoreVar)
             |  WHERE {
             |    Graph schema:Dataset {
             |      ($sameAsVar ?score) text:query (renku:slug schema:keywords schema:description schema:name $luceneQuery).
             |    }
             |  }
             |  group by $sameAsVar
             |}
             |
             |""".stripMargin

      case None =>
        fr"""|  Bind (xsd:float(1.0) as $matchingScoreVar)
             |  Graph schema:Dataset {
             |    $sameAsVar a renku:DiscoverableDataset.
             |  }
            """.stripMargin
    }
}

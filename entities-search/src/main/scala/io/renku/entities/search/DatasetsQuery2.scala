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

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.entities.search.Criteria.Filters
import io.renku.entities.search.model.{Entity, MatchingScore}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model._
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import io.renku.triplesstore.client.syntax._

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

  override def query(criteria: Criteria): Option[String] =
    criteria.filters.whenRequesting(entityType) {
      fr"""{
          |SELECT $entityTypeVar
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
          |    SELECT $sameAsVar (SAMPLE(?projId) as ?projectId)
          |      (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS $creatorsNamesVar)
          |      (GROUP_CONCAT(DISTINCT ?idPathVisibility; separator=',') AS $idsPathsVisibilitiesVar)
          |      (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS $keywordsVar)
          |      (GROUP_CONCAT(DISTINCT ?encodedImageUrl; separator=',') AS $imagesVar)
          |    WHERE {
          |      Graph schema:Dataset {
          |        #creator
          |        Optional {
          |          $sameAsVar schema:creator / schema:name ?creatorName.
          |        }
          |
          |        #keywords, description
          |        $keywords
          |
          |        # resolve project
          |        $resolveProject
          |
          |        # images
          |        $images
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
          |  ${creatorsPart(criteria.filters.creators)}
          |
          |  Graph schema:Dataset {
          |    # name
          |    $sameAsVar renku:slug $nameVar
          |
          |    #description
          |    $description
          |
          |    # dates
          |    ${datesPart(criteria.filters.maybeSince, criteria.filters.maybeUntil)}
          |  }
          |}
          |}
          |""".stripMargin.sparql
    }

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
    fr"""|       OPTIONAL {
         |          ?sameAs schema:image ?imageId .
         |          ?imageId schema:position ?imagePosition ;
         |                   schema:contentUrl ?imageUrl .
         |          BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
         |       }
         |""".stripMargin

  def resolveProject: Fragment =
    fr"""|    $sameAsVar a renku:DiscoverableDataset;
         |            renku:datasetProjectLink / renku:project ?projId.
         |""".stripMargin

  def datesPart(maybeSince: Option[Filters.Since], maybeUntil: Option[Filters.Until]): Fragment = {
    val sinceLocal =
      maybeSince.map(_.value).map(d => fr"$d")
    val untilLocal =
      maybeUntil.map(_.value).map(d => fr"$d")

    def dateCond = {
      val cond =
        List(
          sinceLocal.map(s => fr"$dateVar >= $s"),
          untilLocal.map(s => fr"$dateVar <= $s")
        ).flatten.intercalate(fr" && ")

      if (cond.isEmpty) Fragment.empty
      else fr"FILTER ($cond)"
    }
    // To make sure comparing dates with the same timezone, we strip the timezone from
    // the dateCreated dateTime. The datePublished and the date given in the query is a
    // localdate without a timezone
    fr"""|    OPTIONAL {
         |      $sameAsVar schema:dateCreated $maybeDateCreatedVar.
         |      BIND (xsd:date(substr(str($maybeDateCreatedVar), 1, 10)) AS $dateVar)
         |    }
         |    OPTIONAL {
         |      $sameAsVar schema:datePublished $maybeDatePublishedVar
         |      BIND (xsd:date($maybeDatePublishedVar) AS $dateVar)
         |    }
         |    $dateCond
         |""".stripMargin
  }

  def keywords: Fragment =
    fr"""|OPTIONAL {
         |  $sameAsVar schema:keywords ?keyword
         |}
         |""".stripMargin

  def description: Fragment =
    fr"""| OPTIONAL {
         |   $sameAsVar schema:description $maybeDescriptionVar
         | }""".stripMargin

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
    val matchFrag =
      creators
        .map(c => fr"CONTAINS(LCASE($creatorsNamesVar), ${c.value.toLowerCase})")
        .toList
        .intercalate(Fragment(" || "))

    if (creators.isEmpty) {
      Fragment.empty
    } else
      fr"""FILTER (IF (BOUND($creatorsNamesVar), $matchFrag, false))""".stripMargin
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

  override def decoder[EE >: Entity.Dataset](implicit renkuUrl: RenkuUrl): Decoder[EE] = { implicit cursor =>
    import DecodingTools._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val toListOfIdsPathsAndVisibilities
        : Option[String] => Decoder.Result[NonEmptyList[(projects.Path, projects.Visibility)]] =
      _.map(
        _.split(",")
          .map(_.trim)
          .map { case s"$projectPath:$visibility" =>
            (
              projects.Path.from(projectPath),
              projects.Visibility.from(visibility)
            ).mapN((_, _))
          }
          .toList
          .sequence
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
          .map {
            case head :: tail => NonEmptyList.of(head, tail: _*).some
            case Nil          => None
          }
      ).getOrElse(Option.empty[NonEmptyList[(projects.Path, projects.Visibility)]].asRight)
        .flatMap {
          case Some(tuples) => tuples.asRight
          case None         => DecodingFailure("DS's project path and visibility not found", Nil).asLeft
        }

    for {
      matchingScore <- extract[MatchingScore]("matchingScore")
      name          <- extract[datasets.Name]("name")
      sameAs        <- extract[datasets.TopmostSameAs]("sameAs")
      pathAndVisibility <- extract[Option[String]]("idsPathsVisibilities")
                             .flatMap(toListOfIdsPathsAndVisibilities)
                             .map(_.toList.maxBy(_._2))
      maybeDateCreated   <- extract[Option[datasets.DateCreated]]("maybeDateCreated")
      maybeDatePublished <- extract[Option[datasets.DatePublished]]("maybeDatePublished")
      date <-
        Either.fromOption(maybeDateCreated.orElse(maybeDatePublished), ifNone = DecodingFailure("No dataset date", Nil))
      creators <- extract[Option[String]]("creatorsNames") >>= toListOf[persons.Name, persons.Name.type](persons.Name)
      keywords <-
        extract[Option[String]]("keywords") >>= toListOf[datasets.Keyword, datasets.Keyword.type](datasets.Keyword)
      maybeDesc <- extract[Option[datasets.Description]]("maybeDescription")
      images    <- extract[Option[String]]("images") >>= toListOfImageUris
    } yield Entity.Dataset(
      matchingScore,
      sameAs,
      name,
      pathAndVisibility._2,
      date,
      creators,
      keywords,
      maybeDesc,
      images,
      pathAndVisibility._1
    )
  }
}

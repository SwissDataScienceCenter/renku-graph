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

package io.renku.entities.search

import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.entities.search.Criteria.Filters
import io.renku.entities.search.model.{Entity, MatchingScore}
import io.renku.entities.searchgraphs.concatSeparator
import io.renku.graph.model._
import io.renku.http.server.security.model.AuthUser
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, VarName}
import io.renku.triplesstore.client.syntax._

object DatasetsQuery extends EntityQuery[Entity.Dataset] {
  override val entityType: Filters.EntityType = Filters.EntityType.Dataset

  private val matchingScoreVar        = VarName("matchingScore")
  private val slugVar                 = VarName("slug")
  private val nameVar                 = VarName("name")
  private val idsSlugsVisibilitiesVar = VarName("idsSlugsVisibilities")
  private val sameAsVar               = VarName("sameAs")
  private val maybeDateCreatedVar     = VarName("maybeDateCreated")
  private val maybeDatePublishedVar   = VarName("maybeDatePublished")
  private val maybeDateModified       = VarName("maybeDateModified")
  private val dateVar                 = VarName("date")
  private val creatorsNamesVar        = VarName("creatorsNames")
  private val maybeDescriptionVar     = VarName("maybeDescription")
  private val keywordsVar             = VarName("keywords")
  private val imagesVar               = VarName("images")

  private val projIdVar    = VarName("projId")
  private val authSnippets = SparqlSnippets(projIdVar)

  override val selectVariables: Set[String] = Set(
    entityTypeVar,
    matchingScoreVar,
    slugVar,
    nameVar,
    idsSlugsVisibilitiesVar,
    sameAsVar,
    maybeDateCreatedVar,
    maybeDatePublishedVar,
    maybeDateModified,
    dateVar,
    creatorsNamesVar,
    maybeDescriptionVar,
    keywordsVar,
    imagesVar
  ).map(_.name)

  override def query(criteria: Criteria): Option[Fragment] =
    criteria.filters.whenRequesting(entityType) {
      fr"""{
          |SELECT DISTINCT $entityTypeVar
          |       $matchingScoreVar
          |       $slugVar
          |       $nameVar
          |       $idsSlugsVisibilitiesVar
          |       $sameAsVar
          |       $maybeDateCreatedVar
          |       $maybeDatePublishedVar
          |       $maybeDateModified
          |       $dateVar
          |       $creatorsNamesVar
          |       $maybeDescriptionVar
          |       $keywordsVar
          |       $imagesVar
          |WHERE {
          |  BIND ('dataset' AS $entityTypeVar)
          |
          |  { # start sub select
          |    SELECT DISTINCT $sameAsVar $matchingScoreVar
          |      $creatorsNamesVar
          |      $idsSlugsVisibilitiesVar
          |      $keywordsVar
          |      $imagesVar
          |    WHERE {
          |      # textQuery
          |      ${textQueryPart(criteria.filters.maybeQuery)}
          |
          |      GRAPH schema:Dataset {
          |        #creator
          |        $sameAsVar renku:creatorsNamesConcat $creatorsNamesVar;
          |                   renku:projectsVisibilitiesConcat $idsSlugsVisibilitiesVar.
          |
          |        OPTIONAL { $sameAsVar renku:keywordsConcat $keywordsVar }
          |        OPTIONAL { $sameAsVar renku:imagesConcat $imagesVar }
          |
          |        # resolve project
          |        $resolveProject
          |      }
          |
          |      GRAPH ?projId {
          |        # project namespaces
          |        ${namespacesPart(criteria.filters.namespaces)}
          |
          |        # access restriction
          |        ${accessRightsAndVisibility(criteria.maybeUser, criteria.filters)}
          |      }
          |    }
          |  }# end sub select
          |
          |  ${creatorsPart(criteria.filters.creators)}
          |
          |  GRAPH schema:Dataset {
          |    # name
          |    $sameAsVar renku:slug $slugVar;
          |               schema:name $nameVar.
          |
          |    #description
          |    $description
          |
          |    # dates
          |    ${datesPart(criteria.filters.maybeSince, criteria.filters.maybeUntil)}
          |  }
          |}
          |}
          |""".stripMargin
    }

  private def accessRightsAndVisibility(maybeUser: Option[AuthUser], filters: Criteria.Filters): Fragment =
    maybeUser.map(_.id) -> filters.roles match {
      case Some(userId) -> roles if roles.nonEmpty =>
        fr"""|$projIdVar renku:projectVisibility ?visibility .
             |${visibilitiesPart(filters.visibilities)}
             |${authSnippets.projectsWithRoles(userId, roles)}
             |""".stripMargin
      case maybeUserId -> _ =>
        fr"""|$projIdVar renku:projectVisibility ?visibility .
             |${authSnippets.visibleProjects(maybeUserId, filters.visibilities)}
             |""".stripMargin
    }

  private lazy val visibilitiesPart: Set[projects.Visibility] => Fragment = {
    case vis if vis.nonEmpty => fr"""VALUES (?visibility) {${vis.map(_.value)}}."""
    case _                   => Fragment.empty
  }

  private def resolveProject: Fragment =
    fr"""|    $sameAsVar a renku:DiscoverableDataset;
         |               renku:datasetProjectLink / renku:project ?projId.
         |""".stripMargin

  private def datesPart(maybeSince: Option[Filters.Since], maybeUntil: Option[Filters.Until]): Fragment = {
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
         |      BIND (xsd:date(substr(str($maybeDateCreatedVar), 1, 10)) AS ?createdAt)
         |    }
         |    OPTIONAL {
         |      $sameAsVar schema:datePublished $maybeDatePublishedVar
         |      BIND (xsd:date($maybeDatePublishedVar) AS ?publishedAt)
         |    }
         |    OPTIONAL {
         |      $sameAsVar schema:dateModified $maybeDateModified.
         |      BIND (xsd:date(substr(str($maybeDateModified), 1, 10)) AS ?modifiedAt)
         |    }
         |    BIND(IF (BOUND(?modifiedAt), ?modifiedAt,
         |            IF (BOUND(?createdAt), ?createdAt, ?publishedAt)) AS $dateVar
         |        )
         |
         |    $dateCond
         |""".stripMargin
  }

  private def description: Fragment =
    fr"""| OPTIONAL {
         |   $sameAsVar schema:description $maybeDescriptionVar
         | }""".stripMargin

  private def namespacesPart(ns: Set[projects.Namespace]): Fragment = {
    val matchFrag =
      if (ns.isEmpty) Fragment.empty
      else fr"Values (?namespace) { ${ns.map(_.value)}  }"

    fr"""|  ?projId renku:slug ?projectSlug;
         |          renku:projectNamespace ?namespace.
         |  $matchFrag
      """.stripMargin
  }

  private def creatorsPart(creators: Set[persons.Name]): Fragment = {
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

  private def textQueryPart(mq: Option[Filters.Query]): Fragment =
    mq match {
      case Some(q) =>
        val luceneQuery = LuceneQuery.fuzzy(q.value)
        fr"""|{
             |  SELECT $sameAsVar (MAX(?score) AS $matchingScoreVar)
             |  WHERE {
             |    Graph schema:Dataset {
             |      (?id ?score) text:query (schema:name renku:slug renku:keywordsConcat schema:description $luceneQuery).
             |     {
             |       $sameAsVar a renku:DiscoverableDataset;
             |                  schema:creator ?id
             |     } UNION {
             |       ?id a renku:DiscoverableDataset
             |       BIND (?id AS $sameAsVar)
             |     }
             |    }
             |  }
             | group by $sameAsVar
             |}""".stripMargin

      case None =>
        fr"""|  Bind (xsd:float(1.0) as $matchingScoreVar)
             |  Graph schema:Dataset {
             |    $sameAsVar a renku:DiscoverableDataset.
             |  }
            """.stripMargin
    }

  override def decoder[EE >: Entity.Dataset]: Decoder[EE] = { implicit cursor =>
    import DecodingTools._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val toListOfIdsSlugsAndVisibilities
        : Option[String] => Decoder.Result[NonEmptyList[(projects.Slug, projects.Visibility)]] =
      _.map(
        _.split(concatSeparator)
          .map(_.trim)
          .map { case s"$projectSlug:$visibility" =>
            (projects.Slug.from(projectSlug), projects.Visibility.from(visibility)).mapN((_, _))
          }
          .toList
          .sequence
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
          .map {
            case head :: tail => NonEmptyList.of(head, tail: _*).some
            case Nil          => None
          }
      ).getOrElse(Option.empty[NonEmptyList[(projects.Slug, projects.Visibility)]].asRight)
        .flatMap {
          case Some(tuples) => tuples.asRight
          case None         => DecodingFailure("DS's project slug and visibility not found", Nil).asLeft
        }

    for {
      matchingScore <- read[MatchingScore](matchingScoreVar)
      slug          <- read[datasets.Slug](slugVar)
      name          <- read[datasets.Name](nameVar)
      sameAs        <- read[datasets.TopmostSameAs](sameAsVar)
      slugAndVisibility <- read[Option[String]](idsSlugsVisibilitiesVar)
                             .flatMap(toListOfIdsSlugsAndVisibilities)
                             .map(_.toList.maxBy(_._2))
      maybeDateCreated   <- read[Option[datasets.DateCreated]](maybeDateCreatedVar)
      maybeDatePublished <- read[Option[datasets.DatePublished]](maybeDatePublishedVar)
      maybeDateModified  <- read[Option[datasets.DateCreated]](maybeDateModified)
      date <-
        Either.fromOption(maybeDateCreated.orElse(maybeDatePublished), ifNone = DecodingFailure("No dataset date", Nil))
      creators <- read[Option[String]](creatorsNamesVar) >>= toListOf[persons.Name, persons.Name.type](concatSeparator)
      keywords <-
        read[Option[String]](keywordsVar) >>= toListOf[datasets.Keyword, datasets.Keyword.type](concatSeparator)
      maybeDesc <- read[Option[datasets.Description]](maybeDescriptionVar)
      images    <- read[Option[String]](imagesVar) >>= toListOfImageUris(concatSeparator)
    } yield Entity.Dataset(
      matchingScore,
      sameAs,
      slug,
      name,
      slugAndVisibility._2,
      date,
      maybeDateModified.map(d => datasets.DateModified(d.value)),
      creators,
      keywords,
      maybeDesc,
      images,
      slugAndVisibility._1
    )
  }
}

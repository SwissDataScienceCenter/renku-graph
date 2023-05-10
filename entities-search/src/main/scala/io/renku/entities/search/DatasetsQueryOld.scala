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

import cats.Show
import cats.data.NonEmptyList
import cats.syntax.all._
import io.circe.{Decoder, DecodingFailure}
import io.renku.entities.search.Criteria.Filters
import io.renku.entities.search.Criteria.Filters.EntityType
import io.renku.entities.search.model.{Entity, MatchingScore}
import io.renku.graph.model._
import io.renku.tinytypes._
import io.renku.triplesstore.client.sparql.LuceneQuery
import io.renku.triplesstore.client.syntax._

import java.time.{Instant, ZoneOffset}

private case object DatasetsQueryOld extends EntityQuery[model.Entity.Dataset] {

  override val selectVariables = Set(
    "?entityType",
    "?matchingScore",
    "?name",
    "?idsPathsVisibilities",
    "?sameAs",
    "?maybeDateCreated",
    "?maybeDatePublished",
    "?date",
    "?creatorsNames",
    "?maybeDescription",
    "?keywords",
    "?images"
  )

  override val entityType: EntityType = EntityType.Dataset

  override def query(criteria: Criteria) = (criteria.filters whenRequesting entityType) {
    import criteria._
    // format: off
    s"""|{
        |  SELECT ?entityType ?matchingScore ?name ?idsPathsVisibilities ?sameAs
        |    ?maybeDateCreated ?maybeDatePublished ?date
        |    ?creatorsNames ?maybeDescription ?keywords ?images
        |  WHERE {
        |    {
        |      SELECT ?sameAs ?name ?matchingScore ?maybeDateCreated ?maybeDatePublished ?date
        |        (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS ?creatorsNames)
        |        (GROUP_CONCAT(DISTINCT ?idPathVisibility; separator=',') AS ?idsPathsVisibilities)
        |        (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |        (GROUP_CONCAT(?encodedImageUrl; separator=',') AS ?images)
        |        ?maybeDescription
        |      WHERE {
        |        {
        |          SELECT ?sameAs ?dsId ?projectId ?matchingScore
        |            (GROUP_CONCAT(DISTINCT ?childProjectId; separator='|') AS ?childProjectsIds)
        |            (GROUP_CONCAT(DISTINCT ?projectIdWhereInvalidated; separator='|') AS ?projectsIdsWhereInvalidated)
        |          WHERE {
        |            ${filters.onQuery(
      s"""|            {
          |              SELECT ?dsId (MAX(?score) AS ?matchingScore)
          |              WHERE {
          |                {
          |                  (?id ?score) text:query (renku:slug schema:keywords schema:description schema:name '${filters.query.query}').
          |                } {
          |                  GRAPH ?projectId {
          |                    ?id a schema:Dataset
          |                  }
          |                  BIND (?id AS ?dsId)
          |                } UNION {
          |                  GRAPH ?projectId {
          |                    ?dsId schema:creator ?id;
          |                          a schema:Dataset
          |                  }
          |                }
          |              }
          |              GROUP BY ?dsId
          |            }
          |""")}
        |            GRAPH ?projectId {
        |              ?dsId a schema:Dataset;
        |                    renku:topmostSameAs ?sameAs;
        |                    ^renku:hasDataset ?projectId.
        |            }
        |            OPTIONAL {
        |              GRAPH ?childProjectId {
        |                ?childDsId prov:wasDerivedFrom/schema:url ?dsId;
        |                           ^renku:hasDataset ?childProjectId
        |              }
        |            }
        |            OPTIONAL {
        |              GRAPH ?projectIdWhereInvalidated {
        |                ?dsId prov:invalidatedAtTime ?invalidationTime;
        |                      ^renku:hasDataset ?projectIdWhereInvalidated
        |              }
        |            }
        |          }
        |          GROUP BY ?sameAs ?dsId ?projectId ?matchingScore
        |        }
        |
        |        FILTER (IF (BOUND(?childProjectsIds), !CONTAINS(STR(?childProjectsIds), STR(?projectId)), true))
        |        FILTER (IF (BOUND(?projectsIdsWhereInvalidated), !CONTAINS(STR(?projectsIdsWhereInvalidated), STR(?projectId)), true))
        |
        |        GRAPH ?projectId {
        |          ?projectId renku:projectNamespace ?namespace;
        |                     renku:projectPath ?projectPath.
        |          ?dsId schema:identifier ?identifier;
        |                renku:slug ?name.
        |          ${criteria.maybeOnAccessRightsAndVisibility("?projectId", "?visibility")}
        |          BIND (CONCAT(STR(?identifier), STR(':'), STR(?projectPath), STR(':'), STR(?visibility)) AS ?idPathVisibility)
        |          ${filters.maybeOnNamespace("?namespace")}
        |          OPTIONAL {
        |            ?dsId schema:creator ?creatorId.
        |            GRAPH <${GraphClass.Persons.id}> {
        |              ?creatorId schema:name ?creatorName
        |            }
        |          }
        |          OPTIONAL {
        |            ?dsId schema:dateCreated ?maybeDateCreated.
        |            BIND (?maybeDateCreated AS ?date)
        |          }
        |          OPTIONAL {
        |            ?dsId schema:datePublished ?maybeDatePublished
        |            BIND (?maybeDatePublished AS ?date)
        |          }
        |          ${filters.maybeOnDatasetDates("?maybeDateCreated", "?maybeDatePublished")}
        |          OPTIONAL { ?dsId schema:keywords ?keyword }
        |          OPTIONAL { ?dsId schema:description ?maybeDescription }
        |          OPTIONAL {
        |            ?dsId schema:image ?imageId .
        |            ?imageId schema:position ?imagePosition ;
        |                     schema:contentUrl ?imageUrl .
        |            BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
        |          }
        |        }
        |      }
        |      GROUP BY ?sameAs ?name ?matchingScore ?maybeDateCreated ?maybeDatePublished ?date ?maybeDescription
        |    }
        |    ${filters.maybeOnCreatorsNames("?creatorsNames")}
        |    BIND ('dataset' AS ?entityType)
        |  }
        |}""".stripMargin
    // format: on
  }

  override def decoder[EE >: Entity.Dataset]: Decoder[EE] = { implicit cursor =>
    import DecodingTools._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    lazy val toListOfIdsPathsAndVisibilities
        : Option[String] => Decoder.Result[NonEmptyList[(datasets.Identifier, projects.Path, projects.Visibility)]] =
      _.map(
        _.split(",")
          .map(_.trim)
          .map { case s"$identifier:$projectPath:$visibility" =>
            (datasets.Identifier.from(identifier),
             projects.Path.from(projectPath),
             projects.Visibility.from(visibility)
            ).mapN((_, _, _))
          }
          .toList
          .sequence
          .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
          .map {
            case head :: tail => NonEmptyList.of(head, tail: _*).some
            case Nil          => None
          }
      ).getOrElse(Option.empty[NonEmptyList[(datasets.Identifier, projects.Path, projects.Visibility)]].asRight)
        .flatMap {
          case Some(tuples) => tuples.asRight
          case None         => DecodingFailure("DS's project path and visibility not found", Nil).asLeft
        }

    def selectBroaderVisibilityTuple(
        or: datasets.SameAs
    ): NonEmptyList[(datasets.Identifier, projects.Path, projects.Visibility)] => (datasets.Identifier,
                                                                                   projects.Path,
                                                                                   projects.Visibility
    ) = tuples =>
      tuples
        .find { case (identifier, _, _) => or.show contains identifier.show }
        .getOrElse {
          tuples
            .find(_._3 == projects.Visibility.Public)
            .orElse(tuples.find(_._3 == projects.Visibility.Internal))
            .getOrElse(tuples.head)
        }

    for {
      matchingScore <- extract[MatchingScore]("matchingScore")
      name          <- extract[datasets.Name]("name")
      sameAs        <- extract[datasets.SameAs]("sameAs")
      idPathAndVisibility <- extract[Option[String]]("idsPathsVisibilities")
                               .flatMap(toListOfIdsPathsAndVisibilities)
                               .map(selectBroaderVisibilityTuple(or = sameAs))
      maybeDateCreated   <- extract[Option[datasets.DateCreated]]("maybeDateCreated")
      maybeDatePublished <- extract[Option[datasets.DatePublished]]("maybeDatePublished")
      date <-
        Either.fromOption(maybeDateCreated.orElse(maybeDatePublished), ifNone = DecodingFailure("No dataset date", Nil))
      creators <- extract[Option[String]]("creatorsNames") >>= toListOf[persons.Name, persons.Name.type](persons.Name)
      keywords <-
        extract[Option[String]]("keywords") >>= toListOf[datasets.Keyword, datasets.Keyword.type](datasets.Keyword)
      maybeDesc <- extract[Option[datasets.Description]]("maybeDescription")
      images    <- extract[Option[String]]("images") >>= toListOfImageUris
    } yield Entity.Dataset(matchingScore,
                           Left(idPathAndVisibility._1),
                           name,
                           idPathAndVisibility._3,
                           date,
                           creators,
                           keywords,
                           maybeDesc,
                           images,
                           idPathAndVisibility._2
    )
  }

  private implicit class FiltersOps(filters: Filters) {

    import io.renku.graph.model.views.SparqlLiteralEncoder.sparqlEncode

    lazy val query: LuceneQuery =
      filters.maybeQuery.map(q => LuceneQuery.escape(q.value)).getOrElse(LuceneQuery.queryAll)

    def whenRequesting(entityType: Filters.EntityType, predicates: Boolean*)(query: => String): Option[String] = {
      val typeMatching = filters.entityTypes match {
        case t if t.isEmpty => true
        case t              => t contains entityType
      }
      Option.when(typeMatching && predicates.forall(_ == true))(query)
    }

    def onQuery(snippet: String, matchingScoreVariableName: String = "?matchingScore"): String =
      foldQuery(_ => snippet, s"BIND (xsd:float(1.0) AS $matchingScoreVariableName)")

    private def foldQuery[A](ifPresent: String => A, ifMissing: => A): A =
      if (query.isQueryAll) ifMissing
      else ifPresent(query.query)

    def maybeOnCreatorName(variableName: String): String =
      filters.creators match {
        case creators if creators.isEmpty => ""
        case creators =>
          s"FILTER (IF (BOUND($variableName), LCASE($variableName) IN ${creators.map(_.toLowerCase.asSparqlEncodedLiteral).mkString("(", ", ", ")")}, false))"
      }

    def maybeOnCreatorsNames(variableName: String): String =
      filters.creators match {
        case creators if creators.isEmpty => ""
        case creators =>
          s"""FILTER (IF (BOUND($variableName), ${creators
              .map(c => s"CONTAINS (LCASE($variableName), ${c.toLowerCase.asSparqlEncodedLiteral})")
              .mkString(" || ")} , false))"""
      }

    def maybeOnDatasetDates(dateCreatedVariable: String, datePublishedVariable: String): String =
      List(
        filters.maybeSince map { since =>
          (
            s"""|BIND (${since.encodeAsXsdZonedDate} AS ?sinceZoned)
                |BIND (${since.encodeAsXsdNotZonedDate} AS ?sinceNotZoned)""".stripMargin,
            s"xsd:date($dateCreatedVariable) >= ?sinceZoned",
            s"xsd:date($datePublishedVariable) >= ?sinceNotZoned"
          )
        },
        filters.maybeUntil map { until =>
          (
            s"""|BIND (${until.encodeAsXsdZonedDate} AS ?untilZoned)
                |BIND (${until.encodeAsXsdNotZonedDate} AS ?untilNotZoned)""".stripMargin,
            s"xsd:date($dateCreatedVariable) <= ?untilZoned",
            s"xsd:date($datePublishedVariable) <= ?untilNotZoned"
          )
        }
      ).flatten.foldLeft(List.empty[String], List.empty[String], List.empty[String]) {
        case ((binds, zonedConditions, notZonedConditions), (bind, zonedCondition, notZonedCondition)) =>
          (bind :: binds, zonedCondition :: zonedConditions, notZonedCondition :: notZonedConditions)
      } match {
        case (Nil, Nil, Nil) => ""
        case (binds, zonedConditions, notZonedConditions) =>
          s"""${binds.mkString("\n")}
             |FILTER (
             |  IF (
             |    BOUND($dateCreatedVariable),
             |      ${zonedConditions.mkString(" && ")},
             |      (IF (
             |        BOUND($datePublishedVariable),
             |          ${notZonedConditions.mkString(" && ")},
             |          false
             |      ))
             |  )
             |)""".stripMargin
      }

    private implicit class DateOps(date: LocalDateTinyType) {

      lazy val encodeAsXsdZonedDate: String =
        s"xsd:date(xsd:dateTime('${Instant.from(date.value.atStartOfDay(ZoneOffset.UTC))}'))"

      lazy val encodeAsXsdNotZonedDate: String = s"xsd:date('$date')"
    }

    private implicit class ValueOps[TT <: TinyType](v: TT)(implicit s: Show[TT]) {
      lazy val asSparqlEncodedLiteral: String = s"'${sparqlEncode(v.show)}'"
    }

    private implicit class StringValueOps[TT <: StringTinyType](v: TT)(implicit s: Show[TT]) {
      def toLowerCase(implicit factory: TinyTypeFactory[TT]): TT = factory(v.show.toLowerCase())
    }
  }
}

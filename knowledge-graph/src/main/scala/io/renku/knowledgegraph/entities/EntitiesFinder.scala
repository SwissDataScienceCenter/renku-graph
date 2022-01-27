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

package io.renku.knowledgegraph.entities

import Endpoint.Criteria
import Endpoint.Criteria.Filters
import Endpoint.Criteria.Filters._
import cats.NonEmptyParallel
import cats.effect.Async
import cats.syntax.all._
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.{Paging, PagingResponse}
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import model._
import org.typelevel.log4cats.Logger

private trait EntitiesFinder[F[_]] {
  def findEntities(criteria: Criteria): F[PagingResponse[Entity]]
}

private class EntitiesFinderImpl[F[_]: Async: NonEmptyParallel: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl[F](rdfStoreConfig, timeRecorder)
    with EntitiesFinder[F]
    with Paging[Entity] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.graph.model.{datasets, projects}
  import io.renku.rdfstore.SparqlQuery
  import io.renku.rdfstore.SparqlQuery.Prefixes

  import java.time.{Instant, ZoneOffset}

  override def findEntities(criteria: Criteria): F[PagingResponse[Entity]] = {
    implicit val resultsFinder: PagedResultsFinder[F, Entity] = pagedResultsFinder(query(criteria))
    findPage[F](criteria.paging)
  }

  private def query(criteria: Criteria) = SparqlQuery.of(
    name = "cross-entity search",
    Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema", text -> "text", xsd -> "xsd"),
    s"""|SELECT ?entityType ?matchingScore ?path ?identifier ?name ?visibility ?date 
        |  ?maybeCreatorName ?maybeDescription ?keywords ?visibilities 
        |  ?maybeDateCreated ?maybeDatePublished ?creatorsNames
        |WHERE {
        |  ${subqueries(criteria.filters).mkString(" UNION ")}
        |}
        |${`ORDER BY`(criteria.sorting)}
        |""".stripMargin
  )

  private def subqueries(filters: Filters): List[String] =
    EntityType.all flatMap {
      case EntityType.Project => maybeProjectsQuery(filters)
      case EntityType.Dataset => maybeDatasetsQuery(filters)
    }

  private def maybeProjectsQuery(filters: Filters) = (filters whenRequesting EntityType.Project) {
    s"""|{
        |  SELECT ?entityType ?matchingScore ?name ?path ?visibility ?date ?maybeCreatorName
        |    ?maybeDescription (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |  WHERE { 
        |    {
        |      SELECT ?projectId (MAX(?score) AS ?matchingScore)
        |      WHERE { 
        |        {
        |          (?id ?score) text:query (schema:name schema:keywords schema:description renku:projectNamespaces '${filters.query}')
        |        } {
        |          ?id a schema:Project
        |          BIND (?id AS ?projectId)
        |        } UNION {
        |          ?projectId schema:creator ?id;
        |                     a schema:Project.
        |        }
        |      }
        |      GROUP BY ?projectId
        |    }
        |    BIND ('project' AS ?entityType)
        |    ?projectId schema:name ?name;
        |               renku:projectPath ?path;
        |               renku:projectVisibility ?visibility;
        |               schema:dateCreated ?date.
        |    ${filters.maybeOnVisibility("?visibility")}
        |    ${filters.maybeOnProjectDateCreated("?date")}
        |    OPTIONAL { ?projectId schema:creator/schema:name ?maybeCreatorName }
        |    ${filters.maybeOnCreatorName("?maybeCreatorName")}
        |    OPTIONAL { ?projectId schema:description ?maybeDescription }
        |    OPTIONAL { ?projectId schema:keywords ?keyword }
        |  }
        |  GROUP BY ?entityType ?matchingScore ?name ?path ?visibility ?date ?maybeCreatorName ?maybeDescription
        |}
        |""".stripMargin
  }

  private def maybeDatasetsQuery(filters: Filters) = (filters whenRequesting EntityType.Dataset) {
    s"""|{
        |  SELECT ?entityType ?matchingScore ?identifier ?name ?visibilities 
        |    ?maybeDateCreated ?maybeDatePublished ?date 
        |    ?creatorsNames ?maybeDescription ?keywords
        |  WHERE {
        |    {
        |      SELECT ?entityType ?matchingScore ?identifier ?name
        |        (GROUP_CONCAT(DISTINCT ?visibility; separator=',') AS ?visibilities)
        |        ?maybeDateCreated ?maybeDatePublished ?date
        |        (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS ?creatorsNames)
        |        ?maybeDescription (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |      WHERE {
        |        {
        |          SELECT ?sameAs (SAMPLE(?dsId) AS ?dsIdSample)
        |            (GROUP_CONCAT(DISTINCT ?dsId; separator='|') AS ?dsIds)
        |            (MAX(?dsScore) AS ?matchingScore)
        |          WHERE {
        |            {
        |              SELECT ?sameAs ?projectId ?dsId ?dsScore
        |                (GROUP_CONCAT(DISTINCT ?childProjectId; separator='|') AS ?childProjectsIds)
        |                (GROUP_CONCAT(DISTINCT ?projectIdWhereInvalidated; separator='|') AS ?projectsIdsWhereInvalidated)
        |              WHERE {
        |                {
        |                  SELECT ?dsId (MAX(?score) AS ?dsScore)
        |                  WHERE {
        |                    {
        |                      (?id ?score) text:query (renku:slug schema:keywords schema:description schema:name '${filters.query}').
        |                    } {
        |                      ?id a schema:Dataset
        |                      BIND (?id AS ?dsId)
        |                    } UNION {
        |                      ?dsId schema:creator ?id;
        |                            a schema:Dataset.
        |                    }
        |                  }
        |                  GROUP BY ?dsId
        |                }
        |                ?dsId renku:topmostSameAs ?sameAs;
        |                      ^renku:hasDataset ?projectId.
        |                OPTIONAL {
        |                ?childDsId prov:wasDerivedFrom/schema:url ?dsId;
        |                           ^renku:hasDataset ?childProjectId.
        |                }
        |                OPTIONAL {
        |                  ?dsId prov:invalidatedAtTime ?invalidationTime;
        |                        ^renku:hasDataset ?projectIdWhereInvalidated
        |                }
        |              }
        |              GROUP BY ?sameAs ?dsId ?projectId ?dsScore
        |            }
        |            FILTER (IF (BOUND(?childProjectsIds), !CONTAINS(STR(?childProjectsIds), STR(?projectId)), true))
        |            FILTER (IF (BOUND(?projectsIdsWhereInvalidated), !CONTAINS(STR(?projectsIdsWhereInvalidated), STR(?projectId)), true))
        |          }
        |          GROUP BY ?sameAs
        |        }
        |        BIND ('dataset' AS ?entityType)
        |        BIND (IF (CONTAINS(STR(?dsIds), STR(?sameAs)), ?sameAs, ?dsIdSample) AS ?dsId)
        |        ?dsId schema:identifier ?identifier;
        |              renku:slug ?name.
        |        ?projectId renku:hasDataset ?dsId;
        |                   renku:projectVisibility ?visibility.
        |        ${filters.maybeOnVisibility("?visibility")}
        |        OPTIONAL { ?dsId schema:creator/schema:name ?creatorName }
        |        OPTIONAL { ?dsId schema:dateCreated ?maybeDateCreated }.
        |        OPTIONAL { ?dsId schema:datePublished ?maybeDatePublished }.
        |        ${filters.maybeOnDatasetDates("?maybeDateCreated", "?maybeDatePublished")}
        |        OPTIONAL { ?dsId schema:description ?maybeDescription }
        |        OPTIONAL { ?dsId schema:keywords ?keyword }
        |        BIND (IF (BOUND(?maybeDateCreated), ?maybeDateCreated, ?maybeDatePublished) AS ?date)
        |      }
        |      GROUP BY ?entityType ?matchingScore ?identifier ?name ?maybeDateCreated ?maybeDatePublished ?date ?maybeDescription
        |    }
        |    ${filters.maybeOnCreatorsNames("?creatorsNames")}
        |  }
        |}
        |""".stripMargin
  }

  private implicit class FiltersOps(filters: Filters) {
    import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode

    lazy val query: String = filters.maybeQuery.map(_.value).getOrElse("*")

    def whenRequesting(entityType: Filters.EntityType)(query: => String): Option[String] =
      Option.when(filters.maybeEntityType.forall(_ == entityType))(query)

    def maybeOnCreatorName(variableName: String): String =
      filters.maybeCreator
        .map { creator =>
          s"FILTER (IF (BOUND($variableName), $variableName = '${sparqlEncode(creator.show)}', false))"
        }
        .getOrElse("")

    def maybeOnCreatorsNames(variableName: String): String =
      filters.maybeCreator
        .map { creator =>
          s"FILTER (IF (BOUND($variableName), CONTAINS($variableName, '${sparqlEncode(creator.show)}'), false))"
        }
        .getOrElse("")

    def maybeOnVisibility(variableName: String): String =
      filters.maybeVisibility
        .map(visibility => s"FILTER ($variableName = '$visibility')")
        .getOrElse("")

    def maybeOnProjectDateCreated(variableName: String): String =
      filters.maybeDate
        .map(date => s"""|BIND (${date.encodeAsXsdZonedDate} AS ?dateZoned)
                         |FILTER (xsd:date($variableName) = ?dateZoned)""".stripMargin)
        .getOrElse("")

    def maybeOnDatasetDates(dateCreatedVariable: String, datePublishedVariable: String): String =
      filters.maybeDate
        .map(date => s"""|BIND (${date.encodeAsXsdZonedDate} AS ?dateZoned)
                         |BIND (${date.encodeAsXsdNotZonedDate} AS ?dateNotZoned)
                         |FILTER (
                         |  IF (
                         |    BOUND($dateCreatedVariable), 
                         |      xsd:date($dateCreatedVariable) = ?dateZoned, 
                         |      (IF (
                         |        BOUND($datePublishedVariable), 
                         |          xsd:date($datePublishedVariable) = ?dateNotZoned, 
                         |          false
                         |      ))
                         |  )
                         |)""".stripMargin)
        .getOrElse("")

    private implicit class DateOps(date: Filters.Date) {

      lazy val encodeAsXsdZonedDate: String =
        s"xsd:date(xsd:dateTime('${Instant.from(date.value.atStartOfDay(ZoneOffset.UTC))}'))"

      lazy val encodeAsXsdNotZonedDate: String = s"xsd:date('$date')"
    }
  }

  private def `ORDER BY`(sorting: Criteria.Sorting.By): String = sorting.property match {
    case Criteria.Sorting.ByName          => s"ORDER BY ${sorting.direction}(?name)"
    case Criteria.Sorting.ByDate          => s"ORDER BY ${sorting.direction}(?date)"
    case Criteria.Sorting.ByMatchingScore => s"ORDER BY ${sorting.direction}(?matchingScore)"
  }

  private implicit lazy val recordDecoder: Decoder[Entity] = {
    import Decoder._
    import io.circe.DecodingFailure
    import io.renku.graph.model.users
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

    def toListOf[TT <: StringTinyType, TTF <: TinyTypeFactory[TT]](implicit
        ttFactory: TTF
    ): Option[String] => Decoder.Result[List[TT]] =
      _.map(_.split(',').toList.map(v => ttFactory.from(v)).sequence.map(_.sortBy(_.value))).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(List.empty))

    implicit val projectDecoder: Decoder[Entity.Project] = { cursor =>
      for {
        matchingScore    <- cursor.downField("matchingScore").downField("value").as[MatchingScore]
        path             <- cursor.downField("path").downField("value").as[projects.Path]
        name             <- cursor.downField("name").downField("value").as[projects.Name]
        visibility       <- cursor.downField("visibility").downField("value").as[projects.Visibility]
        dateCreated      <- cursor.downField("date").downField("value").as[projects.DateCreated]
        maybeCreatorName <- cursor.downField("maybeCreatorName").downField("value").as[Option[users.Name]]
        keywords <-
          cursor
            .downField("keywords")
            .downField("value")
            .as[Option[String]]
            .flatMap(toListOf[projects.Keyword, projects.Keyword.type](projects.Keyword))
        maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[projects.Description]]
      } yield Entity.Project(matchingScore,
                             path,
                             name,
                             visibility,
                             dateCreated,
                             maybeCreatorName,
                             keywords,
                             maybeDescription
      )
    }

    implicit val datasetDecoder: Decoder[Entity.Dataset] = { cursor =>
      for {
        matchingScore <- cursor.downField("matchingScore").downField("value").as[MatchingScore]
        identifier    <- cursor.downField("identifier").downField("value").as[datasets.Identifier]
        name          <- cursor.downField("name").downField("value").as[datasets.Name]
        visibility <-
          cursor
            .downField("visibilities")
            .downField("value")
            .as[Option[String]]
            .flatMap(toListOf[projects.Visibility, projects.Visibility.type](projects.Visibility))
            .map(list =>
              list
                .find(_ == projects.Visibility.Public)
                .orElse(list.find(_ == projects.Visibility.Internal))
                .getOrElse(projects.Visibility.Private)
            )
        maybeDateCreated <- cursor.downField("maybeDateCreated").downField("value").as[Option[datasets.DateCreated]]
        maybeDatePublished <-
          cursor.downField("maybeDatePublished").downField("value").as[Option[datasets.DatePublished]]
        date <- Either.fromOption(maybeDateCreated.orElse(maybeDatePublished),
                                  ifNone = DecodingFailure("No dataset date", Nil)
                )
        creators <- cursor
                      .downField("creatorsNames")
                      .downField("value")
                      .as[Option[String]]
                      .flatMap(toListOf[users.Name, users.Name.type](users.Name))
        keywords <- cursor
                      .downField("keywords")
                      .downField("value")
                      .as[Option[String]]
                      .flatMap(toListOf[datasets.Keyword, datasets.Keyword.type](datasets.Keyword))
        maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[datasets.Description]]
      } yield Entity.Dataset(matchingScore, identifier, name, visibility, date, creators, keywords, maybeDescription)
    }

    cursor =>
      cursor.downField("entityType").downField("value").as[String] >>= {
        case "project" => cursor.as[Entity.Project]
        case "dataset" => cursor.as[Entity.Dataset]
      }
  }
}

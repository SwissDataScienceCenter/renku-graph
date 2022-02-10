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
import io.renku.graph.model.plans
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.{Paging, PagingResponse}
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import io.renku.tinytypes.TinyTypeConverter
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
        |  ?maybeDateCreated ?maybeDatePublished ?creatorsNames ?wkId
        |WHERE {
        |  ${subqueries(criteria).mkString(" UNION ")}
        |}
        |${`ORDER BY`(criteria.sorting)}
        |""".stripMargin
  )

  private def subqueries(criteria: Criteria): List[String] =
    EntityType.all flatMap {
      case EntityType.Project  => maybeProjectsQuery(criteria)
      case EntityType.Dataset  => maybeDatasetsQuery(criteria)
      case EntityType.Workflow => maybeWorkflowQuery(criteria)
      case EntityType.Person   => maybePersonsQuery(criteria)
    }

  private def maybeProjectsQuery(criteria: Criteria) = (criteria.filters whenRequesting EntityType.Project) {
    import criteria._
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
        |    ${criteria.maybeOnAccessRights("?projectId", "?visibility")}
        |    ${filters.maybeOnVisibility("?visibility")}
        |    ${filters.maybeOnDateCreated("?date")}
        |    OPTIONAL { ?projectId schema:creator/schema:name ?maybeCreatorName }
        |    ${filters.maybeOnCreatorName("?maybeCreatorName")}
        |    OPTIONAL { ?projectId schema:description ?maybeDescription }
        |    OPTIONAL { ?projectId schema:keywords ?keyword }
        |  }
        |  GROUP BY ?entityType ?matchingScore ?name ?path ?visibility ?date ?maybeCreatorName ?maybeDescription
        |}
        |""".stripMargin
  }

  private def maybeDatasetsQuery(criteria: Criteria) = (criteria.filters whenRequesting EntityType.Dataset) {
    import criteria._
    s"""|{
        |  SELECT ?entityType ?matchingScore ?identifier ?name ?visibilities
        |    ?maybeDateCreated ?maybeDatePublished ?date
        |    ?creatorsNames ?maybeDescription ?keywords
        |  WHERE {
        |    {
        |      SELECT ?sameAs ?matchingScore ?maybeDateCreated ?maybeDatePublished ?date
        |        (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS ?creatorsNames)
        |        (GROUP_CONCAT(DISTINCT ?visibility; separator=',') AS ?visibilities)
        |        (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |        (SAMPLE(?dsId) AS ?dsIdSample) (GROUP_CONCAT(DISTINCT ?dsId; separator='|') AS ?dsIds)
        |      WHERE {
        |        {
        |          SELECT ?sameAs ?dsId ?projectId ?matchingScore
        |            (GROUP_CONCAT(DISTINCT ?childProjectId; separator='|') AS ?childProjectsIds)
        |            (GROUP_CONCAT(DISTINCT ?projectIdWhereInvalidated; separator='|') AS ?projectsIdsWhereInvalidated)
        |          WHERE {
        |            {
        |              SELECT ?dsId (MAX(?score) AS ?matchingScore)
        |              WHERE {
        |                {
        |                  (?id ?score) text:query (renku:slug schema:keywords schema:description schema:name '${filters.query}').
        |                } {
        |                  ?id a schema:Dataset
        |                  BIND (?id AS ?dsId)
        |                } UNION {
        |                  ?dsId schema:creator ?id;
        |                        a schema:Dataset.
        |                }
        |              }
        |              GROUP BY ?dsId
        |            }
        |            ?dsId renku:topmostSameAs ?sameAs;
        |                  ^renku:hasDataset ?projectId.
        |            OPTIONAL {
        |            ?childDsId prov:wasDerivedFrom/schema:url ?dsId;
        |                       ^renku:hasDataset ?childProjectId.
        |            }
        |            OPTIONAL {
        |              ?dsId prov:invalidatedAtTime ?invalidationTime;
        |                    ^renku:hasDataset ?projectIdWhereInvalidated
        |            }
        |          }
        |          GROUP BY ?sameAs ?dsId ?projectId ?matchingScore
        |        }
        |        FILTER (IF (BOUND(?childProjectsIds), !CONTAINS(STR(?childProjectsIds), STR(?projectId)), true))
        |        FILTER (IF (BOUND(?projectsIdsWhereInvalidated), !CONTAINS(STR(?projectsIdsWhereInvalidated), STR(?projectId)), true))
        |        ?projectId renku:projectVisibility ?visibility.
        |        ${criteria.maybeOnAccessRights("?projectId", "?visibility")}
        |        ${filters.maybeOnVisibility("?visibility")}
        |        OPTIONAL { ?dsId schema:creator/schema:name ?creatorName }
        |        OPTIONAL { ?dsId schema:dateCreated ?maybeDateCreated }.
        |        OPTIONAL { ?dsId schema:datePublished ?maybeDatePublished }.
        |        BIND (IF (BOUND(?maybeDateCreated), ?maybeDateCreated, ?maybeDatePublished) AS ?date)
        |        ${filters.maybeOnDatasetDates("?maybeDateCreated", "?maybeDatePublished")}
        |        OPTIONAL { ?dsId schema:keywords ?keyword }
        |      }
        |      GROUP BY ?sameAs ?matchingScore ?maybeDateCreated ?maybeDatePublished ?date
        |    }
        |    ${filters.maybeOnCreatorsNames("?creatorsNames")}
        |    BIND ('dataset' AS ?entityType)
        |    BIND (IF (CONTAINS(STR(?dsIds), STR(?sameAs)), ?sameAs, ?dsIdSample) AS ?dsId)
        |    ?dsId schema:identifier ?identifier;
        |          renku:slug ?name.
        |    OPTIONAL { ?dsId schema:description ?maybeDescription }
        |  }
        |}""".stripMargin
  }

  private def maybeWorkflowQuery(criteria: Criteria) = (criteria.filters whenRequesting EntityType.Workflow) {
    import criteria._
    s"""|{
        |  SELECT ?entityType ?matchingScore ?wkId ?name ?visibilities ?date ?maybeDescription
        |    (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |  WHERE {
        |    {
        |      SELECT ?wkId ?matchingScore ?date (GROUP_CONCAT(DISTINCT ?visibility; separator=',') AS ?visibilities)
        |      WHERE {
        |        {
        |          SELECT ?wkId (MAX(?score) AS ?matchingScore)
        |          WHERE {
        |            (?wkId ?score) text:query (schema:name schema:keywords schema:description '${filters.query}').
        |            ?wkId a prov:Plan
        |          }
        |          GROUP BY ?wkId
        |        }
        |        ?wkId schema:name ?name;
        |              schema:dateCreated ?date;
        |              ^renku:hasPlan ?projectId.
        |        ?projectId renku:projectVisibility ?visibility
        |        ${criteria.maybeOnAccessRights("?projectId", "?visibility")}
        |        ${filters.maybeOnVisibility("?visibility")}
        |        ${filters.maybeOnDateCreated("?date")}
        |      }
        |      GROUP BY ?wkId ?matchingScore ?date
        |    }
        |    BIND ('workflow' AS ?entityType)
        |    ?wkId schema:name ?name.
        |    OPTIONAL { ?wkId schema:description ?maybeDescription }
        |    OPTIONAL { ?wkId schema:keywords ?keyword }
        |  }
        |  GROUP BY ?entityType ?matchingScore ?wkId ?name ?visibilities ?date ?maybeDescription
        |}
        |""".stripMargin
  }

  private def maybePersonsQuery(criteria: Criteria) =
    (criteria.filters whenRequesting (EntityType.Person, criteria.filters.withNoOrPublicVisibility, criteria.filters.maybeDate.isEmpty)) {
      import criteria._
      s"""|{
          |  SELECT DISTINCT ?entityType ?matchingScore ?name
          |  WHERE {
          |    {
          |      SELECT (SAMPLE(?id) AS ?personId) ?name (MAX(?score) AS ?matchingScore)
          |      WHERE { 
          |        (?id ?score) text:query (schema:name '${filters.query}').
          |        ?id a schema:Person;
          |            schema:name ?name.
          |        ${filters.maybeOnCreatorName("?name")}    
          |      }
          |      GROUP BY ?name
          |    }
          |    BIND ('person' AS ?entityType)
          |  }
          |}
          |""".stripMargin
    }

  private implicit class FiltersOps(filters: Filters) {
    import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode

    lazy val query: String = filters.maybeQuery.map(_.value).getOrElse("*")

    def whenRequesting(entityType: Filters.EntityType, predicates: Boolean*)(query: => String): Option[String] =
      Option.when(filters.maybeEntityType.forall(_ == entityType) && predicates.forall(_ == true))(query)

    lazy val withNoOrPublicVisibility: Boolean = filters.maybeVisibility forall (_ == projects.Visibility.Public)

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

    def maybeOnDateCreated(variableName: String): String =
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

  private implicit class CriteriaOps(criteria: Criteria) {

    def maybeOnAccessRights(projectIdVariable: String, visibilityVariable: String): String = criteria.maybeUser match {
      case Some(user) =>
        s"""|OPTIONAL {
            |    $projectIdVariable schema:member/schema:sameAs ?memberId.
            |    ?memberId schema:additionalType 'GitLab';
            |              schema:identifier ?userGitlabId .
            |}
            |FILTER (
            |  $visibilityVariable = '${projects.Visibility.Public.value}' || ?userGitlabId = ${user.id.value}
            |)
            |""".stripMargin
      case _ =>
        s"""FILTER ($visibilityVariable = '${projects.Visibility.Public.value}')"""
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
    import io.renku.graph.model.persons
    import io.renku.tinytypes.json.TinyTypeDecoders._
    import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

    def toListOf[TT <: StringTinyType, TTF <: TinyTypeFactory[TT]](implicit
        ttFactory: TTF
    ): Option[String] => Decoder.Result[List[TT]] =
      _.map(_.split(',').toList.map(v => ttFactory.from(v)).sequence.map(_.sortBy(_.value))).sequence
        .leftMap(ex => DecodingFailure(ex.getMessage, Nil))
        .map(_.getOrElse(List.empty))

    lazy val selectBroaderVisibility: List[projects.Visibility] => projects.Visibility = list =>
      list
        .find(_ == projects.Visibility.Public)
        .orElse(list.find(_ == projects.Visibility.Internal))
        .getOrElse(projects.Visibility.Private)

    implicit val projectDecoder: Decoder[Entity.Project] = { cursor =>
      for {
        matchingScore    <- cursor.downField("matchingScore").downField("value").as[MatchingScore]
        path             <- cursor.downField("path").downField("value").as[projects.Path]
        name             <- cursor.downField("name").downField("value").as[projects.Name]
        visibility       <- cursor.downField("visibility").downField("value").as[projects.Visibility]
        dateCreated      <- cursor.downField("date").downField("value").as[projects.DateCreated]
        maybeCreatorName <- cursor.downField("maybeCreatorName").downField("value").as[Option[persons.Name]]
        keywords <- cursor
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
        visibility <- cursor
                        .downField("visibilities")
                        .downField("value")
                        .as[Option[String]]
                        .flatMap(toListOf[projects.Visibility, projects.Visibility.type](projects.Visibility))
                        .map(selectBroaderVisibility)
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
                      .flatMap(toListOf[persons.Name, persons.Name.type](persons.Name))
        keywords <- cursor
                      .downField("keywords")
                      .downField("value")
                      .as[Option[String]]
                      .flatMap(toListOf[datasets.Keyword, datasets.Keyword.type](datasets.Keyword))
        maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[datasets.Description]]
      } yield Entity.Dataset(matchingScore, identifier, name, visibility, date, creators, keywords, maybeDescription)
    }

    implicit val workflowDecoder: Decoder[Entity.Workflow] = { cursor =>
      for {
        matchingScore <- cursor.downField("matchingScore").downField("value").as[MatchingScore]
        resourceId    <- cursor.downField("wkId").downField("value").as[plans.ResourceId]
        name          <- cursor.downField("name").downField("value").as[plans.Name]
        dateCreated   <- cursor.downField("date").downField("value").as[plans.DateCreated]
        identifier <- implicitly[TinyTypeConverter[plans.ResourceId, plans.Identifier]]
                        .apply(resourceId)
                        .leftMap(e => DecodingFailure(e.getMessage, Nil))
        visibility <- cursor
                        .downField("visibilities")
                        .downField("value")
                        .as[Option[String]]
                        .flatMap(toListOf[projects.Visibility, projects.Visibility.type](projects.Visibility))
                        .map(selectBroaderVisibility)
        keywords <- cursor
                      .downField("keywords")
                      .downField("value")
                      .as[Option[String]]
                      .flatMap(toListOf[plans.Keyword, plans.Keyword.type](plans.Keyword))
        maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[plans.Description]]
      } yield Entity.Workflow(matchingScore, identifier, name, visibility, dateCreated, keywords, maybeDescription)
    }

    implicit val personDecoder: Decoder[Entity.Person] = { cursor =>
      for {
        matchingScore <- cursor.downField("matchingScore").downField("value").as[MatchingScore]
        name          <- cursor.downField("name").downField("value").as[persons.Name]
      } yield Entity.Person(matchingScore, name)
    }

    cursor =>
      cursor.downField("entityType").downField("value").as[EntityType] >>= {
        case EntityType.Project  => cursor.as[Entity.Project]
        case EntityType.Dataset  => cursor.as[Entity.Dataset]
        case EntityType.Workflow => cursor.as[Entity.Workflow]
        case EntityType.Person   => cursor.as[Entity.Person]
      }
  }
}

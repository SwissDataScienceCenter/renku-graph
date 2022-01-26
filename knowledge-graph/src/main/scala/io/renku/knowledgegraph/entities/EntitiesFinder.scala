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

import Endpoint.Filters._
import Endpoint._
import cats.effect.Async
import cats.syntax.all._
import io.renku.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQueryTimeRecorder}
import model._
import org.typelevel.log4cats.Logger

private trait EntitiesFinder[F[_]] {
  def findEntities(filters: Filters): F[List[Entity]]
}

private class EntitiesFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl[F](rdfStoreConfig, timeRecorder, printQueries = true)
    with EntitiesFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas._
  import io.renku.graph.model.{datasets, projects}
  import io.renku.rdfstore.SparqlQuery
  import io.renku.rdfstore.SparqlQuery.Prefixes

  import java.time.{Instant, ZoneOffset}

  override def findEntities(filters: Filters): F[List[Entity]] =
    queryExpecting[List[Entity]](using = query(filters))

  private def query(filters: Filters) =
    SparqlQuery.of(
      name = "cross-entity search",
      Prefixes.of(renku -> "renku", schema -> "schema", text -> "text", xsd -> "xsd"),
      s"""|SELECT ?entityType ?matchingScore ?name ?path ?visibility ?dateCreated 
          |  ?maybeCreatorName ?maybeDescription ?keywords ?visibilities 
          |  ?maybeDateCreated ?maybeDatePublished ?creatorsNames
          |WHERE {
          |  ${subqueries(filters).mkString(" UNION ")}
          |}
          |ORDER BY ?name
          |""".stripMargin
    )

  private def subqueries(filters: Filters): List[String] =
    EntityType.all flatMap {
      case EntityType.Project => maybeProjectsQuery(filters)
      case EntityType.Dataset => maybeDatasetsQuery(filters)
    }

  private def maybeProjectsQuery(filters: Filters) = (filters whenRequesting EntityType.Project) {
    s"""|{
        |  SELECT ?entityType ?matchingScore ?name ?path ?visibility ?dateCreated ?maybeCreatorName
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
        |               schema:dateCreated ?dateCreated.
        |    ${filters.maybeOnVisibility("?visibility")}
        |    ${filters.maybeOnProjectDateCreated("?dateCreated")}
        |    OPTIONAL { ?projectId schema:creator/schema:name ?maybeCreatorName }
        |    ${filters.maybeOnCreatorName("?maybeCreatorName")}
        |    OPTIONAL { ?projectId schema:description ?maybeDescription }
        |    OPTIONAL { ?projectId schema:keywords ?keyword }
        |  }
        |  GROUP BY ?entityType ?matchingScore ?name ?path ?visibility ?dateCreated ?maybeCreatorName ?maybeDescription
        |}
        |""".stripMargin
  }

  private def maybeDatasetsQuery(filters: Filters) = (filters whenRequesting EntityType.Dataset) {
    s"""|{
        |  SELECT ?entityType ?matchingScore ?name ?visibilities ?maybeDateCreated 
        |    ?maybeDatePublished ?creatorsNames ?maybeDescription ?keywords
        |  WHERE {
        |    {
        |      SELECT ?entityType ?matchingScore ?name (GROUP_CONCAT(DISTINCT ?visibility; separator=',') AS ?visibilities)
        |        ?maybeDateCreated ?maybeDatePublished
        |        (GROUP_CONCAT(DISTINCT ?creatorName; separator=',') AS ?creatorsNames)
        |        ?maybeDescription (GROUP_CONCAT(DISTINCT ?keyword; separator=',') AS ?keywords)
        |      WHERE {
        |         {
        |          SELECT ?dsId (MAX(?score) AS ?matchingScore)
        |          WHERE {
        |            {
        |              (?id ?score) text:query (renku:slug schema:keywords schema:description schema:name '${filters.query}').
        |            } {
        |              ?id a schema:Dataset
        |              BIND (?id AS ?dsId)
        |            } UNION {
        |              ?dsId schema:creator ?id;
        |                    a schema:Dataset.
        |            }
        |          }
        |          GROUP BY ?dsId
        |        }
        |        BIND ('dataset' AS ?entityType)
        |        ?dsId renku:slug ?name.
        |        ?projectId renku:hasDataset ?dsId;
        |                   renku:projectVisibility ?visibility.
        |        ${filters.maybeOnVisibility("?visibility")}
        |        OPTIONAL { ?dsId schema:creator/schema:name ?creatorName }
        |        OPTIONAL { ?dsId schema:dateCreated ?maybeDateCreated }.
        |        OPTIONAL { ?dsId schema:datePublished ?maybeDatePublished }.
        |        ${filters.maybeOnDatasetDates("?maybeDateCreated", "?maybeDatePublished")}
        |        OPTIONAL { ?dsId schema:description ?maybeDescription }
        |        OPTIONAL { ?dsId schema:keywords ?keyword }
        |      }
        |      GROUP BY ?entityType ?matchingScore ?name ?maybeDateCreated ?maybeDatePublished ?maybeDescription
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
        .map(date => s"""|BIND (${date.encodeAsXsdZonedDate} AS ?date)
                         |FILTER (xsd:date($variableName) = ?date)""".stripMargin)
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

  private implicit lazy val recordsDecoder: Decoder[List[Entity]] = {
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
        name             <- cursor.downField("name").downField("value").as[projects.Name]
        path             <- cursor.downField("path").downField("value").as[projects.Path]
        visibility       <- cursor.downField("visibility").downField("value").as[projects.Visibility]
        dateCreated      <- cursor.downField("dateCreated").downField("value").as[projects.DateCreated]
        maybeCreatorName <- cursor.downField("maybeCreatorName").downField("value").as[Option[users.Name]]
        keywords <-
          cursor
            .downField("keywords")
            .downField("value")
            .as[Option[String]]
            .flatMap(toListOf[projects.Keyword, projects.Keyword.type](projects.Keyword))
        maybeDescription <- cursor.downField("maybeDescription").downField("value").as[Option[projects.Description]]
      } yield Entity.Project(matchingScore,
                             name,
                             path,
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
      } yield Entity.Dataset(matchingScore, name, visibility, date, creators, keywords, maybeDescription)
    }

    val entity: Decoder[Entity] = { cursor =>
      cursor.downField("entityType").downField("value").as[String] >>= {
        case "project" => cursor.as[Entity.Project]
        case "dataset" => cursor.as[Entity.Dataset]
      }
    }

    _.downField("results").downField("bindings").as(decodeList(entity))
  }
}

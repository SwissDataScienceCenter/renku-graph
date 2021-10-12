/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets.rest

import cats.Parallel
import cats.effect.{ConcurrentEffect, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.datasets.{Date, DateCreated, DatePublished, Description, Identifier, ImageUri, Keyword, Name, Title}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import ch.datascience.tinytypes.constraints.NonNegativeInt
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import eu.timepit.refined.auto._
import io.circe.DecodingFailure
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait DatasetsFinder[Interpretation[_]] {
  def findDatasets(maybePhrase: Option[Phrase],
                   sort:        Sort.By,
                   paging:      PagingRequest,
                   maybeUser:   Option[AuthUser]
  ): Interpretation[PagingResponse[DatasetSearchResult]]
}

private object DatasetsFinder {

  final case class DatasetSearchResult(
      id:                  Identifier,
      title:               Title,
      name:                Name,
      maybeDescription:    Option[Description],
      creators:            Set[DatasetCreator],
      date:                Date,
      exemplarProjectPath: projects.Path,
      projectsCount:       ProjectsCount,
      keywords:            List[Keyword],
      images:              List[ImageUri]
  )

  final class ProjectsCount private (val value: Int) extends AnyVal with IntTinyType

  implicit object ProjectsCount extends TinyTypeFactory[ProjectsCount](new ProjectsCount(_)) with NonNegativeInt
}

private class DatasetsFinderImpl[Interpretation[_]: ConcurrentEffect: Timer: Parallel](
    rdfStoreConfig:          RdfStoreConfig,
    creatorsFinder:          CreatorsFinder[Interpretation],
    logger:                  Logger[Interpretation],
    timeRecorder:            SparqlQueryTimeRecorder[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder)
    with DatasetsFinder[Interpretation]
    with Paging[DatasetSearchResult] {

  import DatasetsFinderImpl._
  import cats.syntax.all._
  import creatorsFinder._

  private lazy val projectMemberFilterQuery: Option[AuthUser] => String = {
    case Some(user) =>
      s"""|?projectId renku:projectVisibility ?visibility .
          |OPTIONAL { 
          |    ?projectId schema:member/schema:sameAs ?memberId.
          |    ?memberId  schema:additionalType 'GitLab';
          |               schema:identifier ?userGitlabId .
          |}
          |FILTER (
          |  ?visibility = '${Visibility.Public.value}' || ?userGitlabId = ${user.id.value}
          |)
          |""".stripMargin
    case _ =>
      s"""|?projectId renku:projectVisibility ?visibility .
          |FILTER(?visibility = '${Visibility.Public.value}')
          |""".stripMargin
  }
  private lazy val addCreators: DatasetSearchResult => Interpretation[DatasetSearchResult] =
    dataset =>
      findCreators(dataset.id)
        .map(creators => dataset.copy(creators = creators))

  override def findDatasets(maybePhrase:   Option[Phrase],
                            sort:          Sort.By,
                            pagingRequest: PagingRequest,
                            maybeUser:     Option[AuthUser]
  ): Interpretation[PagingResponse[DatasetSearchResult]] = {
    val phrase = maybePhrase getOrElse Phrase("*")
    implicit val resultsFinder: PagedResultsFinder[Interpretation, DatasetSearchResult] = pagedResultsFinder(
      sparqlQuery(phrase, sort, maybeUser)
    )
    for {
      page                 <- findPage[Interpretation](pagingRequest)
      datasetsWithCreators <- (page.results map addCreators).parSequence
      updatedPage          <- page.updateResults[Interpretation](datasetsWithCreators)
    } yield updatedPage
  }

  private def sparqlQuery(phrase: Phrase, sort: Sort.By, maybeUser: Option[AuthUser]): SparqlQuery = SparqlQuery.of(
    name = "ds free-text search",
    Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema", text -> "text"),
    s"""|SELECT ?identifier ?name ?slug ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?exemplarProjectPath ?projectsCount (GROUP_CONCAT(?keyword; separator='|') AS ?keywords) ?images
        |WHERE {        
        |  SELECT ?identifier ?name ?slug ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?exemplarProjectPath ?projectsCount ?keyword (GROUP_CONCAT(?encodedImageUrl; separator=',') AS ?images)
        |  WHERE {
        |    {
        |      SELECT ?sameAs (COUNT(DISTINCT ?projectId) AS ?projectsCount) (MIN(?projectDate) AS ?minProjectDate)
        |      WHERE {
        |        {
        |          SELECT DISTINCT ?sameAs
        |          WHERE {
        |            {
        |              SELECT DISTINCT ?id
        |              WHERE { ?id text:query (schema:name schema:description renku:slug schema:keywords '$phrase') }
        |            } {
        |              ?id a schema:Dataset;
        |              	   renku:topmostSameAs ?sameAs.
        |            } UNION {
        |              ?id a schema:Person.
        |              ?luceneDsId schema:creator ?id;
        |                          a schema:Dataset;
        |                          renku:topmostSameAs ?sameAs.
        |            }
        |          }
        |        } {
        |          ?dsId a schema:Dataset;
        |                ^renku:hasDataset ?projectId;
        |                renku:topmostSameAs ?sameAs.
        |          FILTER NOT EXISTS {
        |            ?dsId prov:invalidatedAtTime ?invalidationTime.
        |          }
        |          FILTER NOT EXISTS {
        |            ?someId prov:wasDerivedFrom/schema:url ?dsId;
        |                    ^renku:hasDataset ?projectId; 
        |          }
        |          ?projectId schema:dateCreated ?projectDate.
        |          ${projectMemberFilterQuery(maybeUser)}
        |        }
        |      }
        |      GROUP BY ?sameAs
        |      HAVING (COUNT(*) > 0)
        |    } {
        |      ?dsIdExample a schema:Dataset;
        |                   renku:topmostSameAs ?sameAs;
        |                   schema:identifier ?identifier;
        |                   schema:name ?name;
        |                   renku:slug ?slug;
        |                   ^renku:hasDataset ?exemplarProjectId.
        |      ?exemplarProjectId schema:dateCreated ?minProjectDate;
        |                         renku:projectPath ?exemplarProjectPath.
        |      OPTIONAL {
        |        ?dsIdExample schema:image ?imageId .
        |        ?imageId schema:position ?imagePosition ;
        |                 schema:contentUrl ?imageUrl .
        |        BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
        |      }
        |      OPTIONAL { ?dsIdExample schema:keywords ?keyword }
        |      OPTIONAL { ?dsIdExample schema:description ?maybeDescription }
        |      OPTIONAL { ?dsIdExample schema:datePublished ?maybePublishedDate }
        |      OPTIONAL { ?dsIdExample schema:dateCreated ?maybeDateCreated }
        |      OPTIONAL { ?dsIdExample prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }
        |      BIND (IF(BOUND(?maybePublishedDate), ?maybePublishedDate, ?maybeDateCreated) AS ?date)
        |      FILTER NOT EXISTS {
        |        ?someId prov:wasDerivedFrom/schema:url ?dsIdExample;
        |                ^renku:hasDataset ?exemplarProjectId.
        |      }
        |    }
        |  }
        |  GROUP BY ?identifier ?name ?slug ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?exemplarProjectPath ?projectsCount ?keyword
        |}
        |GROUP BY ?identifier ?name ?slug ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?exemplarProjectPath ?projectsCount ?images
        |${`ORDER BY`(sort)}
        |""".stripMargin
  )

  private def `ORDER BY`(sort: Sort.By): String = sort.property match {
    case Sort.TitleProperty         => s"ORDER BY ${sort.direction}(?name)"
    case Sort.DateProperty          => s"ORDER BY ${sort.direction}(?date)"
    case Sort.DatePublishedProperty => s"ORDER BY ${sort.direction}(?maybePublishedDate)"
    case Sort.ProjectsCountProperty => s"ORDER BY ${sort.direction}(?projectsCount)"
  }
}

private object DatasetsFinderImpl {
  import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.ProjectsCount
  import io.circe.Decoder

  implicit val recordDecoder: Decoder[DatasetSearchResult] = { cursor =>
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    def toListOfImageUrls(urlString: Option[String]): List[ImageUri] =
      urlString
        .map(
          _.split(",")
            .map(_.trim)
            .map { case s"$position:$url" => position.toIntOption.getOrElse(0) -> ImageUri(url) }
            .toSet[(Int, ImageUri)]
            .toList
            .sortBy(_._1)
            .map(_._2)
        )
        .getOrElse(Nil)

    def toListOfKeywords(keywordsString: Option[String]): List[Keyword] =
      keywordsString
        .map(
          _.split("\\|")
            .map(_.trim)
            .toSet
            .toList
            .map(Keyword.apply)
            .sorted
        )
        .getOrElse(Nil)

    for {
      id                  <- cursor.downField("identifier").downField("value").as[Identifier]
      title               <- cursor.downField("name").downField("value").as[Title]
      name                <- cursor.downField("slug").downField("value").as[Name]
      maybeDateCreated    <- cursor.downField("maybeDateCreated").downField("value").as[Option[DateCreated]]
      maybePublishedDate  <- cursor.downField("maybePublishedDate").downField("value").as[Option[DatePublished]]
      projectsCount       <- cursor.downField("projectsCount").downField("value").as[ProjectsCount]
      exemplarProjectPath <- cursor.downField("exemplarProjectPath").downField("value").as[projects.Path]
      keywords            <- cursor.downField("keywords").downField("value").as[Option[String]].map(toListOfKeywords)
      images              <- cursor.downField("images").downField("value").as[Option[String]].map(toListOfImageUrls)
      maybeDescription    <- cursor.downField("maybeDescription").downField("value").as[Option[Description]]
      date <- maybeDateCreated
                .orElse(maybePublishedDate)
                .map(_.asRight)
                .getOrElse(DecodingFailure("No dateCreated or publishedDate found", Nil).asLeft)
    } yield DatasetSearchResult(
      id,
      title,
      name,
      maybeDescription,
      Set.empty,
      date,
      exemplarProjectPath,
      projectsCount,
      keywords,
      images
    )
  }
}

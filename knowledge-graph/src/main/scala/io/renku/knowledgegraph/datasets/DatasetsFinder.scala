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

package io.renku.knowledgegraph.datasets

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.DecodingFailure
import io.circe.literal._
import io.renku.graph.config.RenkuUrlLoader
import io.renku.graph.model.Schemas._
import io.renku.graph.model.datasets.{DateCreated, DatePublished, Description, Identifier, Keyword, Name, Slug}
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.Visibility
import io.renku.graph.model.{GraphClass, RenkuUrl, projects}
import io.renku.http.rest.Sorting
import io.renku.http.rest.paging.Paging.PagedResultsFinder
import io.renku.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.datasets.DatasetSearchResult.ExemplarProject
import io.renku.knowledgegraph.datasets.Endpoint.Query.Phrase
import io.renku.knowledgegraph.datasets.Endpoint.Sort
import io.renku.projectauth.util.SparqlSnippets
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.OrderBy
import io.renku.triplesstore.client.sparql.{SparqlEncoder, VarName}
import org.typelevel.log4cats.Logger

private trait DatasetsFinder[F[_]] {
  def findDatasets(maybePhrase: Option[Phrase],
                   sort:        Sorting[Endpoint.Sort.type],
                   paging:      PagingRequest,
                   maybeUser:   Option[AuthUser]
  ): F[PagingResponse[DatasetSearchResult]]
}

private object DatasetsFinder {

  def apply[F[_]: Parallel: Async: Logger: SparqlQueryTimeRecorder](storeConfig:    ProjectsConnectionConfig,
                                                                    creatorsFinder: CreatorsFinder[F]
  ): F[DatasetsFinder[F]] =
    RenkuUrlLoader[F]().map(implicit renkuUrl => new DatasetsFinderImpl[F](storeConfig, creatorsFinder))
}

private class DatasetsFinderImpl[F[_]: Parallel: Async: Logger: SparqlQueryTimeRecorder](
    storeConfig:    ProjectsConnectionConfig,
    creatorsFinder: CreatorsFinder[F]
)(implicit renkuUrl: RenkuUrl)
    extends TSClientImpl[F](storeConfig)
    with DatasetsFinder[F]
    with Paging[DatasetSearchResult] {

  import DatasetsFinderImpl._
  import creatorsFinder._

  override def findDatasets(maybePhrase:   Option[Phrase],
                            sort:          Sorting[Endpoint.Sort.type],
                            pagingRequest: PagingRequest,
                            maybeUser:     Option[AuthUser]
  ): F[PagingResponse[DatasetSearchResult]] = {
    val phrase = maybePhrase getOrElse Phrase("*")
    implicit val finder: PagedResultsFinder[F, DatasetSearchResult] = pagedResultsFinder(query(phrase, sort, maybeUser))
    for {
      page                 <- findPage[F](pagingRequest)
      datasetsWithCreators <- (page.results map addCreators).parSequence
      updatedPage          <- page.updateResults[F](datasetsWithCreators)
    } yield updatedPage
  }

  private def query(phrase: Phrase, sort: Sorting[Endpoint.Sort.type], maybeUser: Option[AuthUser]): SparqlQuery =
    SparqlQuery.of(
      name = "ds free-text search",
      Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema", text -> "text"),
      s"""|SELECT ?identifier ?name ?slug ?maybeDescription ?maybeDatePublished ?maybeDateCreated ?date 
          |  ?maybeDerivedFrom ?sameAs (SAMPLE(?projectSlug) AS ?projectSampleSlug) ?projectsCount
          |  (GROUP_CONCAT(?keyword; separator='|') AS ?keywords)
          |  (GROUP_CONCAT(?encodedImageUrl; separator='|') AS ?images) 
          |WHERE { 
          |  { 
          |    SELECT ?sameAs (COUNT(DISTINCT ?projectId) AS ?projectsCount) 
          |      (SAMPLE(?dsId) AS ?dsIdSample) (SAMPLE(?projectId) AS ?projectIdSample) 
          |    WHERE { 
          |      { 
          |        SELECT ?sameAs ?projectId ?dsId 
          |          (GROUP_CONCAT(DISTINCT ?childProjectId; separator='|') AS ?childProjectsIds) 
          |          (GROUP_CONCAT(DISTINCT ?projectIdWhereInvalidated; separator='|') AS ?projectsIdsWhereInvalidated)
          |        WHERE {
          |          {
          |            SELECT DISTINCT ?projectId ?id
          |            WHERE { ?id text:query (schema:name schema:description renku:slug schema:keywords '$phrase') }
          |          } {
          |            GRAPH ?projectId {
          |              ?id a schema:Dataset
          |            }
          |            BIND(?id AS ?dsId)
          |          } UNION {
          |            GRAPH <${GraphClass.Persons.id}> {
          |              ?id a schema:Person
          |            }
          |            GRAPH ?projectId {
          |              ?dsId schema:creator ?id;
          |                    a schema:Dataset
          |            }
          |          }
          |          ${projectMemberFilterQuery(maybeUser)}
          |          GRAPH ?projectId {
          |            ?dsId renku:topmostSameAs ?sameAs;
          |                  ^renku:hasDataset ?projectId.
          |            OPTIONAL {
          |              ?childDsId prov:wasDerivedFrom/schema:url ?dsId;
          |                         ^renku:hasDataset ?childProjectId.
          |            }
          |            OPTIONAL {
          |              ?dsId prov:invalidatedAtTime ?invalidationTime;
          |                    ^renku:hasDataset ?projectIdWhereInvalidated
          |            }
          |          }
          |        }
          |        GROUP BY ?sameAs ?projectId ?dsId
          |      }
          |      FILTER (IF (BOUND(?childProjectsIds), !CONTAINS(STR(?childProjectsIds), STR(?projectId)), true))
          |      FILTER (IF (BOUND(?projectsIdsWhereInvalidated), !CONTAINS(STR(?projectsIdsWhereInvalidated), STR(?projectId)), true))
          |    }
          |    GROUP BY ?sameAs 
          |  }
          |  GRAPH ?projectIdSample {
          |    ?dsIdSample renku:topmostSameAs ?sameAs;
          |                schema:identifier ?identifier;
          |                schema:name ?name;
          |                renku:slug ?slug. 
          |    ?projectIdSample renku:projectPath ?projectSlug.
          |    OPTIONAL {
          |      ?dsIdSample schema:image ?imageId.
          |      ?imageId schema:position ?imagePosition;
          |               schema:contentUrl ?imageUrl.
          |      BIND (CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
          |    }
          |    OPTIONAL { ?dsIdSample schema:keywords ?keyword }
          |    OPTIONAL { ?dsIdSample schema:description ?maybeDescription }
          |    OPTIONAL { ?dsIdSample prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }
          |    OPTIONAL {
          |      ?dsIdSample schema:dateCreated ?maybeDateCreated.
          |      BIND (?maybeDateCreated AS ?date)
          |    }
          |    OPTIONAL {
          |      ?dsIdSample schema:datePublished ?maybeDatePublished
          |      BIND (?maybeDatePublished AS ?date)
          |    }
          |  }
          |}
          |GROUP BY ?identifier ?name ?slug ?maybeDescription ?maybeDatePublished ?maybeDateCreated ?date
          |  ?maybeDerivedFrom ?sameAs ?projectsCount
          |${`ORDER BY`(sort)}
          |""".stripMargin
    )

  private lazy val projectMemberFilterQuery: Option[AuthUser] => String = { user =>
    SparqlSnippets(VarName("projectId")).visibleProjects(user.map(_.id), Visibility.all).sparql
  }

  private def `ORDER BY`(sort: Sorting[Endpoint.Sort.type])(implicit encoder: SparqlEncoder[OrderBy]): String = {
    def mapPropertyName(property: Sort.SearchProperty) = property match {
      case Sort.NameProperty          => OrderBy.Property("?name")
      case Sort.DateProperty          => OrderBy.Property("?date")
      case Sort.DatePublishedProperty => OrderBy.Property("?maybeDatePublished")
      case Sort.ProjectsCountProperty => OrderBy.Property("?projectsCount")
    }

    encoder(sort.toOrderBy(mapPropertyName)).sparql
  }

  private lazy val addCreators: DatasetSearchResult => F[DatasetSearchResult] = dataset =>
    findCreators(dataset.id, dataset.exemplarProject.id).map(creators => dataset.copy(creators = creators.toList))
}

private object DatasetsFinderImpl {
  import DatasetSearchResult.ProjectsCount
  import io.circe.Decoder

  implicit def recordsDecoder(implicit renkuUrl: RenkuUrl): Decoder[DatasetSearchResult] = { cursor =>
    import io.renku.tinytypes.json.TinyTypeDecoders._

    def toListOfImageUrls(urlString: Option[String]): List[ImageUri] =
      urlString
        .map(
          _.split("\\|")
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
      name                <- cursor.downField("name").downField("value").as[Name]
      slug                <- cursor.downField("slug").downField("value").as[Slug]
      maybeDateCreated    <- cursor.downField("maybeDateCreated").downField("value").as[Option[DateCreated]]
      maybePublishedDate  <- cursor.downField("maybeDatePublished").downField("value").as[Option[DatePublished]]
      projectsCount       <- cursor.downField("projectsCount").downField("value").as[ProjectsCount]
      exemplarProjectSlug <- cursor.downField("projectSampleSlug").downField("value").as[projects.Slug]
      keywords            <- cursor.downField("keywords").downField("value").as[Option[String]].map(toListOfKeywords)
      images              <- cursor.downField("images").downField("value").as[Option[String]].map(toListOfImageUrls)
      maybeDescription    <- cursor.downField("maybeDescription").downField("value").as[Option[Description]]
      date <- maybeDateCreated
                .orElse(maybePublishedDate)
                .map(_.asRight)
                .getOrElse(DecodingFailure("No dateCreated or datePublished found", Nil).asLeft)
    } yield DatasetSearchResult(
      id,
      name,
      slug,
      maybeDescription,
      List.empty[DatasetCreator],
      date,
      ExemplarProject(projects.ResourceId(exemplarProjectSlug), exemplarProjectSlug),
      projectsCount,
      keywords,
      images
    )
  }
}

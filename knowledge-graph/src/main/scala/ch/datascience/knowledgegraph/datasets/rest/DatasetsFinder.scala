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

import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.datasets.{DateCreated, Dates, Description, Identifier, ImageUri, Keyword, Name, PublishedDate, Title}
import ch.datascience.graph.model.projects.Visibility
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import ch.datascience.http.server.security.model.AuthUser
import ch.datascience.knowledgegraph.datasets.model.DatasetCreator
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.rdfstore._
import ch.datascience.tinytypes.constraints.NonNegativeInt
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger
import io.circe.DecodingFailure

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
      id:               Identifier,
      title:            Title,
      name:             Name,
      maybeDescription: Option[Description],
      creators:         Set[DatasetCreator],
      dates:            Dates,
      projectsCount:    ProjectsCount,
      keywords:         List[Keyword],
      images:           List[ImageUri]
  )

  final class ProjectsCount private (val value: Int) extends AnyVal with IntTinyType

  implicit object ProjectsCount extends TinyTypeFactory[ProjectsCount](new ProjectsCount(_)) with NonNegativeInt

}

private class IODatasetsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    creatorsFinder:          CreatorsFinder,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with DatasetsFinder[IO]
    with Paging[IO, DatasetSearchResult] {

  import IODatasetsFinder._
  import cats.syntax.all._
  import creatorsFinder._

  override def findDatasets(maybePhrase:   Option[Phrase],
                            sort:          Sort.By,
                            pagingRequest: PagingRequest,
                            maybeUser:     Option[AuthUser]
  ): IO[PagingResponse[DatasetSearchResult]] = {
    val phrase = maybePhrase getOrElse Phrase("*")
    implicit val resultsFinder: PagedResultsFinder[IO, DatasetSearchResult] = pagedResultsFinder(
      sparqlQuery(phrase, sort, maybeUser)
    )
    for {
      page                 <- findPage(pagingRequest)
      datasetsWithCreators <- (page.results map addCreators).parSequence
      updatedPage          <- page.updateResults[IO](datasetsWithCreators)
    } yield updatedPage
  }

  private lazy val projectMemberFilterQuery: Option[AuthUser] => String = {
    case Some(user) =>
      s"""
         |OPTIONAL { 
         |    ?projectId renku:projectVisibility ?visibility;
         |               schema:member/schema:sameAs ?memberId.
         |    ?memberId  schema:additionalType 'GitLab';
         |               schema:identifier ?userGitlabId .
         |}
         |BIND (IF (BOUND (?visibility), ?visibility,  '${Visibility.Public.value}') as ?projectVisibility)
         |FILTER (
         |  ?projectVisibility = '${Visibility.Public.value}' || 
         |  ((?projectVisibility = '${Visibility.Private.value}' || ?projectVisibility = '${Visibility.Internal.value}') && 
         |  ?userGitlabId = ${user.id.value})
         |)
         |""".stripMargin
    case _ =>
      s"""
         |OPTIONAL {
         |  ?projectId renku:projectVisibility ?visibility .
         |}
         |BIND(IF (BOUND (?visibility), ?visibility, '${Visibility.Public.value}' ) as ?projectVisibility)
         |FILTER(?projectVisibility = '${Visibility.Public.value}')
         |""".stripMargin
  }

  private def sparqlQuery(phrase: Phrase, sort: Sort.By, maybeUser: Option[AuthUser]): SparqlQuery = SparqlQuery(
    name = "ds free-text search",
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    s"""|SELECT ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?projectsCount (GROUP_CONCAT(?keyword; separator='|') AS ?keywords) ?images
        |WHERE {        
        |  SELECT ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?projectsCount ?keyword (GROUP_CONCAT(?encodedImageUrl; separator=',') AS ?images)
        |  WHERE {
        |    {
        |      SELECT (MIN(?dsId) AS ?dsIdExample) ?sameAs (COUNT(DISTINCT ?projectId) AS ?projectsCount)
        |      WHERE {
        |        {
        |          SELECT DISTINCT ?sameAs
        |          WHERE {
        |            {
        |              SELECT DISTINCT ?id
        |              WHERE { ?id text:query (schema:name schema:description schema:alternateName schema:keywords '$phrase') }
        |            } {
        |              ?id rdf:type <http://schema.org/Dataset>;
        |              	renku:topmostSameAs ?sameAs.
        |            } UNION {
        |              ?id rdf:type <http://schema.org/Person>.
        |              ?luceneDsId schema:creator ?id;
        |                          rdf:type <http://schema.org/Dataset>;
        |                          renku:topmostSameAs ?sameAs.
        |            }
        |          }
        |        } {
        |          ?dsId rdf:type <http://schema.org/Dataset>;
        |                renku:topmostSameAs ?sameAs;
        |                schema:isPartOf ?projectId ;
        |                prov:atLocation ?location .
        |          BIND(CONCAT(?location, "/metadata.yml") AS ?metaDataLocation).
        |          FILTER NOT EXISTS {
        |              # Removing dataset that have an activity that invalidates them
        |              ?deprecationEntity rdf:type <http://www.w3.org/ns/prov#Entity>;
        |                                 prov:atLocation ?metaDataLocation ;
        |                                 prov:wasInvalidatedBy ?invalidationActivity ;
        |                                 schema:isPartOf ?projectId .
        |          }
        |          FILTER NOT EXISTS {
        |              ?someId prov:wasDerivedFrom/schema:url ?dsId.
        |              ?someId schema:isPartOf ?projectId.
        |          }
        |          ${projectMemberFilterQuery(maybeUser)}
        |        }
        |      }
        |      GROUP BY ?sameAs
        |      HAVING (COUNT(*) > 0)
        |    } {
        |      ?dsIdExample rdf:type <http://schema.org/Dataset>;
        |            renku:topmostSameAs ?sameAs;
        |            schema:identifier ?identifier;
        |            schema:name ?name ;
        |            schema:alternateName ?alternateName;
        |            schema:isPartOf ?projectId .
        |      OPTIONAL {
        |        ?dsIdExample schema:image ?imageId .
        |        ?imageId     schema:position ?imagePosition ;
        |                     schema:contentUrl ?imageUrl .
        |        BIND(CONCAT(STR(?imagePosition), STR(':'), STR(?imageUrl)) AS ?encodedImageUrl)
        |      }
        |      OPTIONAL { ?dsIdExample schema:keywords ?keyword }
        |      OPTIONAL { ?dsIdExample schema:description ?maybeDescription }
        |      OPTIONAL { ?dsIdExample schema:datePublished ?maybePublishedDate }
        |      OPTIONAL { ?dsIdExample schema:dateCreated ?maybeDateCreated }
        |      OPTIONAL { ?dsIdExample prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }
        |      OPTIONAL { ?dsIdExample schema:url ?maybeUrl }
        |      BIND (IF(BOUND(?maybePublishedDate), ?maybePublishedDate, ?maybeDateCreated) AS ?date)
        |      FILTER NOT EXISTS {
        |        ?someId prov:wasDerivedFrom/schema:url ?dsIdExample.
        |        ?someId schema:isPartOf ?projectId.
        |      }
        |    }
        |  }
        |  GROUP BY ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?projectsCount ?keyword
        |}
        |GROUP BY ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?maybeDateCreated ?date ?maybeDerivedFrom ?sameAs ?projectsCount ?images
        |${`ORDER BY`(sort)}
        |""".stripMargin
  )

  private def `ORDER BY`(sort: Sort.By): String = sort.property match {
    case Sort.TitleProperty         => s"ORDER BY ${sort.direction}(?name)"
    case Sort.DateProperty          => s"ORDER BY ${sort.direction}(?date)"
    case Sort.DatePublishedProperty => s"ORDER BY ${sort.direction}(?maybePublishedDate)"
    case Sort.ProjectsCountProperty => s"ORDER BY ${sort.direction}(?projectsCount)"
  }

  private lazy val addCreators: DatasetSearchResult => IO[DatasetSearchResult] =
    dataset =>
      findCreators(dataset.id)
        .map(creators => dataset.copy(creators = creators))
}

private object IODatasetsFinder {
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
      id                 <- cursor.downField("identifier").downField("value").as[Identifier]
      title              <- cursor.downField("name").downField("value").as[Title]
      name               <- cursor.downField("alternateName").downField("value").as[Name]
      maybeDateCreated   <- cursor.downField("maybeDateCreated").downField("value").as[Option[DateCreated]]
      maybePublishedDate <- cursor.downField("maybePublishedDate").downField("value").as[Option[PublishedDate]]
      projectsCount      <- cursor.downField("projectsCount").downField("value").as[ProjectsCount]
      keywords           <- cursor.downField("keywords").downField("value").as[Option[String]].map(toListOfKeywords)
      images             <- cursor.downField("images").downField("value").as[Option[String]].map(toListOfImageUrls)
      maybeDescription <- cursor
                            .downField("maybeDescription")
                            .downField("value")
                            .as[Option[String]]
                            .map(blankToNone)
                            .flatMap(toOption[Description])
      dates <- Dates
                 .from(maybeDateCreated, maybePublishedDate)
                 .leftMap(e => DecodingFailure(e.getMessage, Nil))
    } yield DatasetSearchResult(
      id,
      title,
      name,
      maybeDescription,
      Set.empty,
      dates,
      projectsCount,
      keywords,
      images
    )
  }
}

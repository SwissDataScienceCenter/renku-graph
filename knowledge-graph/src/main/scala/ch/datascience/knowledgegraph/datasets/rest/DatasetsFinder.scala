/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PublishedDate, Title}
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import ch.datascience.knowledgegraph.datasets.model.DatasetPublishing
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.rdfstore._
import ch.datascience.tinytypes.constraints.NonNegativeInt
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.predicates.all.NonEmpty
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DatasetsFinder[Interpretation[_]] {
  def findDatasets(maybePhrase: Option[Phrase],
                   sort:        Sort.By,
                   paging:      PagingRequest
  ): Interpretation[PagingResponse[DatasetSearchResult]]
}

private object DatasetsFinder {

  final case class DatasetSearchResult(
      id:               Identifier,
      title:            Title,
      name:             Name,
      maybeDescription: Option[Description],
      published:        DatasetPublishing,
      projectsCount:    ProjectsCount
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
                            pagingRequest: PagingRequest
  ): IO[PagingResponse[DatasetSearchResult]] = {
    val phrase = maybePhrase getOrElse Phrase("*")
    implicit val resultsFinder: PagedResultsFinder[IO, DatasetSearchResult] = pagedResultsFinder(
      sparqlQuery(phrase, sort)
    )
    for {
      page                 <- findPage(pagingRequest)
      datasetsWithCreators <- (page.results map addCreators).parSequence
      updatedPage          <- page.updateResults[IO](datasetsWithCreators)
    } yield updatedPage
  }

  private def sparqlQuery(phrase: Phrase, sort: Sort.By): SparqlQuery = SparqlQuery(
    name = queryName(phrase, "ds free-text search"),
    Set(
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    s"""|SELECT DISTINCT ?identifier ?name ?alternateName ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs ?projectsCount
        |WHERE {
        |  {
        |    SELECT (MIN(?dsId) AS ?dsIdExample) ?sameAs (COUNT(DISTINCT ?projectId) AS ?projectsCount)
        |    WHERE {
        |      {
        |        SELECT DISTINCT ?sameAs
        |        WHERE {
        |          {
        |            SELECT DISTINCT ?id
        |            WHERE { ?id text:query (schema:name schema:description schema:alternateName '$phrase') }
        |          } {
        |            ?id rdf:type <http://schema.org/Dataset>;
        |            	renku:topmostSameAs ?sameAs.
        |          } UNION {
        |            ?id rdf:type <http://schema.org/Person>.
        |            ?luceneDsId schema:creator ?id;
        |                        rdf:type <http://schema.org/Dataset>;
        |                        renku:topmostSameAs ?sameAs.
        |          }
        |        }
        |      } {
        |        ?dsId rdf:type <http://schema.org/Dataset>;
        |              renku:topmostSameAs ?sameAs;
        |              schema:isPartOf ?projectId.
        |        FILTER NOT EXISTS {
        |          ?someId prov:wasDerivedFrom/schema:url ?dsId.
        |          ?someId schema:isPartOf ?projectId.
        |        }
        |      }
        |    }
        |    GROUP BY ?sameAs
        |    HAVING (COUNT(*) > 0)
        |  } {
        |    ?dsIdExample rdf:type <http://schema.org/Dataset>;
        |          renku:topmostSameAs ?sameAs;
        |          schema:identifier ?identifier;
        |          schema:name ?name ;
        |          schema:alternateName ?alternateName;
        |          schema:isPartOf ?projectId.
        |    OPTIONAL { ?dsIdExample schema:description ?maybeDescription }
        |    OPTIONAL { ?dsIdExample schema:datePublished ?maybePublishedDate }
        |    OPTIONAL { ?dsIdExample prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }
        |    OPTIONAL { ?dsIdExample schema:url ?maybeUrl }
        |        FILTER NOT EXISTS {
        |      ?someId prov:wasDerivedFrom/schema:url ?dsIdExample.
        |      ?someId schema:isPartOf ?projectId.
        |    }
        |  }
        |}
        |${`ORDER BY`(sort)}
        |""".stripMargin
  )

  private def `ORDER BY`(sort: Sort.By): String = sort.property match {
    case Sort.TitleProperty         => s"ORDER BY ${sort.direction}(?name)"
    case Sort.DatePublishedProperty => s"ORDER BY ${sort.direction}(?maybePublishedDate)"
    case Sort.ProjectsCountProperty => s"ORDER BY ${sort.direction}(?projectsCount)"
  }

  private lazy val addCreators: DatasetSearchResult => IO[DatasetSearchResult] =
    dataset =>
      findCreators(dataset.id)
        .map(creators => dataset.copy(published = dataset.published.copy(creators = creators)))

  private def queryName(phrase: Phrase, name: String Refined NonEmpty): String Refined NonEmpty =
    phrase.value match {
      case "*" => Refined.unsafeApply(s"$name *")
      case _   => name
    }
}

private object IODatasetsFinder {
  import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.ProjectsCount
  import io.circe.Decoder

  implicit val recordDecoder: Decoder[DatasetSearchResult] = { cursor =>
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    for {
      id                 <- cursor.downField("identifier").downField("value").as[Identifier]
      title              <- cursor.downField("name").downField("value").as[Title]
      name               <- cursor.downField("alternateName").downField("value").as[Name]
      maybePublishedDate <- cursor.downField("maybePublishedDate").downField("value").as[Option[PublishedDate]]
      projectsCount      <- cursor.downField("projectsCount").downField("value").as[ProjectsCount]
      maybeDescription <- cursor
                            .downField("maybeDescription")
                            .downField("value")
                            .as[Option[String]]
                            .map(blankToNone)
                            .flatMap(toOption[Description])
    } yield DatasetSearchResult(
      id,
      title,
      name,
      maybeDescription,
      DatasetPublishing(maybePublishedDate, Set.empty),
      projectsCount
    )
  }
}

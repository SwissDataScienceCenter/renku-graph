/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PublishedDate}
import ch.datascience.knowledgegraph.datasets.CreatorsFinder
import ch.datascience.knowledgegraph.datasets.model.DatasetPublishing
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.rdfstore.IORdfStoreClient.RdfQuery
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig}
import ch.datascience.tinytypes.constraints.NonNegative
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import io.chrisdavenport.log4cats.Logger
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DatasetsFinder[Interpretation[_]] {
  def findDatasets(phrase: Phrase, sort: Sort.By): Interpretation[List[DatasetSearchResult]]
}

private object DatasetsFinder {
  final case class DatasetSearchResult(
      id:               Identifier,
      name:             Name,
      maybeDescription: Option[Description],
      published:        DatasetPublishing,
      projectsCount:    ProjectsCount
  )

  final class ProjectsCount private (val value: Int) extends AnyVal with IntTinyType
  implicit object ProjectsCount extends TinyTypeFactory[ProjectsCount](new ProjectsCount(_)) with NonNegative
}

private class IODatasetsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    creatorsFinder:          CreatorsFinder,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient[RdfQuery](rdfStoreConfig, logger)
    with DatasetsFinder[IO] {

  import IODatasetsFinder._
  import cats.implicits._
  import creatorsFinder._

  override def findDatasets(phrase: Phrase, sort: Sort.By): IO[List[DatasetSearchResult]] =
    for {
      datasets             <- queryExpecting[List[DatasetSearchResult]](using = query(phrase, sort))
      datasetsWithCreators <- (datasets map addCreators).parSequence
    } yield datasetsWithCreators

  private def query(phrase: Phrase, sort: Sort.By): String =
    s"""
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX schema: <http://schema.org/>
       |PREFIX text: <http://jena.apache.org/text#>
       |
       |SELECT ?identifier ?name ?maybeDescription ?maybePublishedDate (COUNT(?project) AS ?projectsCount)
       |WHERE {
       |  {
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             text:query (schema:name '$phrase') ;
       |             schema:name ?name ;
       |             schema:identifier ?identifier ;
       |             schema:isPartOf ?project .
       |    OPTIONAL { ?dataset schema:description ?maybeDescription } .
       |    OPTIONAL { ?dataset schema:datePublished ?maybePublishedDate } .
       |  } UNION {
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             text:query (schema:description '$phrase') ;
       |             schema:name ?name ;
       |             schema:identifier ?identifier ;
       |             schema:isPartOf ?project .
       |    OPTIONAL { ?dataset schema:description ?maybeDescription } .
       |    OPTIONAL { ?dataset schema:datePublished ?maybePublishedDate } .
       |  } UNION {
       |    ?dataset rdf:type <http://schema.org/Dataset> ;
       |             schema:creator ?creatorResource ;
       |             schema:identifier ?identifier ;
       |             schema:name ?name ;
       |             schema:isPartOf ?project .
       |    ?creatorResource rdf:type <http://schema.org/Person> ;
       |             text:query (schema:name '$phrase') .
       |    OPTIONAL { ?dataset schema:description ?maybeDescription } .
       |    OPTIONAL { ?dataset schema:datePublished ?maybePublishedDate } .
       |  }
       |}
       |GROUP BY ?identifier ?name ?maybeDescription ?maybePublishedDate
       |ORDER BY ${sort.direction}(?${sort.property})""".stripMargin

  private lazy val addCreators: DatasetSearchResult => IO[DatasetSearchResult] =
    dataset =>
      findCreators(dataset.id)
        .map(creators => dataset.copy(published = dataset.published.copy(creators = creators)))
}

private object IODatasetsFinder {
  import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.ProjectsCount
  import io.circe.Decoder

  private implicit val recordsDecoder: Decoder[List[DatasetSearchResult]] = {
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    implicit val recordDecoder: Decoder[DatasetSearchResult] = { cursor =>
      for {
        id                 <- cursor.downField("identifier").downField("value").as[Identifier]
        name               <- cursor.downField("name").downField("value").as[Name]
        maybePublishedDate <- cursor.downField("maybePublishedDate").downField("value").as[Option[PublishedDate]]
        projectsCount      <- cursor.downField("projectsCount").downField("value").as[ProjectsCount]
        maybeDescription <- cursor
                             .downField("maybeDescription")
                             .downField("value")
                             .as[Option[String]]
                             .map(blankToNone)
                             .flatMap(toOption[Description])
      } yield
        DatasetSearchResult(
          id,
          name,
          maybeDescription,
          DatasetPublishing(maybePublishedDate, Set.empty),
          projectsCount
        )
    }

    _.downField("results").downField("bindings").as(decodeList[DatasetSearchResult])
  }
}

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

import cats.MonadError
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.datasets.{Identifier, ImageUrl, Keyword}
import ch.datascience.knowledgegraph.datasets.model.Dataset
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger
import io.circe.HCursor

import scala.concurrent.ExecutionContext

private class BaseDetailsFinder(
    rdfStoreConfig: RdfStoreConfig,
    logger:         Logger[IO],
    timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit
    executionContext: ExecutionContext,
    contextShift:     ContextShift[IO],
    timer:            Timer[IO],
    ME:               MonadError[IO, Throwable]
) extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder) {

  import BaseDetailsFinder._
  import ch.datascience.graph.Schemas._

  def findBaseDetails(identifier: Identifier): IO[Option[Dataset]] =
    queryExpecting[List[Dataset]](using = queryForDatasetDetails(identifier)) flatMap { dataset =>
      toSingleDataset(dataset)
    }

  private def queryForDatasetDetails(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - details",
    Prefixes.of(
      prov   -> "prov",
      rdf    -> "rdf",
      renku  -> "renku",
      schema -> "schema"
    ),
    s"""|SELECT DISTINCT ?identifier ?name ?alternateName ?url ?topmostSameAs ?maybeDerivedFrom ?initialVersion ?description ?publishedDate ?imageIdentifier
        |WHERE {
        |    ?datasetId schema:identifier "$identifier";
        |               schema:identifier ?identifier;
        |               rdf:type <http://schema.org/Dataset>;
        |               schema:isPartOf ?projectId;   
        |               schema:url ?url;
        |               schema:name ?name;
        |               schema:alternateName ?alternateName;
        |               prov:atLocation ?location ;
        |               renku:topmostSameAs ?topmostSameAs;
        |               renku:topmostDerivedFrom/schema:identifier ?initialVersion
        |               
        |    BIND(CONCAT(?location, "/metadata.yml") AS ?metaDataLocation) .
        |    FILTER NOT EXISTS {
        |      # Removing dataset that have an activity that invalidates them
        |      ?deprecationEntity rdf:type <http://www.w3.org/ns/prov#Entity>;
        |                         prov:atLocation ?metaDataLocation ;
        |                         prov:wasInvalidatedBy ?invalidationActivity ;
        |                         schema:isPartOf ?projectId .
        |    }          
        |               
        |    OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
        |    OPTIONAL { ?datasetId schema:description ?description }.
        |    OPTIONAL { ?datasetId schema:datePublished ?publishedDate }.
        |    
        |}
        |""".stripMargin
  )

  def findKeywords(identifier: Identifier): IO[List[Keyword]] =
    queryExpecting[List[Keyword]](using = queryKeywords(identifier)).flatMap(s => ME.pure(s))

  private def queryKeywords(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - keyword details",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?keyword
        |WHERE {
        |    ?datasetId schema:identifier "$identifier" ;
        |               schema:keywords ?keyword .
        |}ORDER BY ASC(?keyword)
        |""".stripMargin
  )

  def findImages(identifier: Identifier): IO[List[ImageUrl]] =
    queryExpecting[List[ImageUrl]](using = queryImages(identifier))

  private def queryImages(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - image urls",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?contentUrl
        |WHERE {
        |    ?datasetId schema:identifier "$identifier" ;
        |               schema:image ?imageId .
        |    ?imageId   a schema:ImageObject;
        |               schema:contentUrl ?contentUrl ;
        |               schema:position ?position .
        |}ORDER BY ASC(?position)
        |""".stripMargin
  )

  private lazy val toSingleDataset: List[Dataset] => IO[Option[Dataset]] = {
    case Nil            => Option.empty[Dataset].pure[IO]
    case dataset :: Nil => Option(dataset).pure[IO]
    case dataset :: _   => new Exception(s"More than one dataset with ${dataset.id} id").raiseError[IO, Option[Dataset]]
  }
}

private object BaseDetailsFinder {
  import io.circe.Decoder
  import Decoder._
  import ch.datascience.graph.model.datasets._
  import ch.datascience.knowledgegraph.datasets.model._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  private[rest] implicit val datasetsDecoder: Decoder[List[Dataset]] = {
    val dataset: Decoder[Dataset] = { implicit cursor =>
      for {
        identifier         <- extract[Identifier]("identifier")
        title              <- extract[Title]("name")
        name               <- extract[Name]("alternateName")
        url                <- extract[Url]("url")
        maybeDerivedFrom   <- extract[Option[DerivedFrom]]("maybeDerivedFrom")
        sameAs             <- extract[SameAs]("topmostSameAs")
        initialVersion     <- extract[InitialVersion]("initialVersion")
        maybePublishedDate <- extract[Option[PublishedDate]]("publishedDate")
        maybeDescription <- extract[Option[String]]("description")
                              .map(blankToNone)
                              .flatMap(toOption[Description])
      } yield maybeDerivedFrom match {
        case Some(derivedFrom) =>
          ModifiedDataset(
            identifier,
            title,
            name,
            url,
            derivedFrom,
            DatasetVersions(initialVersion),
            maybeDescription,
            DatasetPublishing(maybePublishedDate, Set.empty),
            parts = List.empty,
            projects = List.empty,
            keywords = List.empty,
            images = List.empty
          )
        case None =>
          NonModifiedDataset(
            identifier,
            title,
            name,
            url,
            sameAs,
            DatasetVersions(initialVersion),
            maybeDescription,
            DatasetPublishing(maybePublishedDate, Set.empty),
            parts = List.empty,
            projects = List.empty,
            keywords = List.empty,
            images = List.empty
          )
      }
    }

    _.downField("results").downField("bindings").as(decodeList(dataset))
  }

  private implicit lazy val keywordsDecoder: Decoder[List[Keyword]] = {

    implicit val keywordDecoder: Decoder[Keyword] =
      _.downField("keyword").downField("value").as[String].map(Keyword.apply)

    _.downField("results").downField("bindings").as(decodeList[Keyword])
  }

  private implicit lazy val imagesDecoder: Decoder[List[ImageUrl]] = {

    implicit val imageDecoder: Decoder[ImageUrl] =
      _.downField("contentUrl").downField("value").as[String].map(ImageUrl.apply)

    _.downField("results").downField("bindings").as(decodeList[ImageUrl])
  }

  private def extract[T](property: String)(implicit cursor: HCursor, decoder: Decoder[T]): Result[T] =
    cursor.downField(property).downField("value").as[T]
}

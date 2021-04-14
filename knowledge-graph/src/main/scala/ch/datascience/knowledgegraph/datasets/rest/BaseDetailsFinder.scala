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
import ch.datascience.graph.model.datasets.{Identifier, ImageUri, Keyword}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.knowledgegraph.datasets.model.{Dataset, DatasetProject}
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import org.typelevel.log4cats.Logger
import io.circe.{DecodingFailure, HCursor}

import scala.concurrent.ExecutionContext
import scala.util.Try

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

  def findBaseDetails(identifier: Identifier, usedIn: List[DatasetProject]): IO[Option[Dataset]] =
    queryExpecting[List[Dataset]](using = queryForDatasetDetails(identifier))(datasetsDecoder(usedIn)) flatMap {
      dataset =>
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
    s"""|SELECT DISTINCT ?identifier ?name ?maybeDateCreated ?alternateName ?url ?topmostSameAs ?maybeDerivedFrom ?initialVersion ?description ?maybePublishedDate ?projectId
        |WHERE {        
        |    {
        |         SELECT  ?projectId  ?dateCreated
        |         WHERE {
        |             ?datasetId rdf:type <http://schema.org/Dataset>;
        |                  schema:identifier '$identifier';
        |                  prov:atLocation ?location ;
        |                  schema:isPartOf ?projectId .
        |             ?projectId schema:dateCreated ?dateCreated ;
        |                        schema:name ?projectName .
        |             BIND(CONCAT(?location, "/metadata.yml") AS ?metaDataLocation) .
        |             FILTER NOT EXISTS {
        |             # Removing dataset that have an activity that invalidates them
        |               ?deprecationEntity rdf:type <http://www.w3.org/ns/prov#Entity>;
        |                                prov:atLocation ?metaDataLocation ;
        |                                prov:wasInvalidatedBy ?invalidationActivity ;
        |                                schema:isPartOf ?projectId .
        |             }  
        |         }
        |         ORDER BY ?dateCreated ?projectName
        |         LIMIT 1
        |    }
        |  
        |  
        |    ?datasetId schema:identifier '$identifier';
        |               schema:identifier ?identifier;
        |               rdf:type <http://schema.org/Dataset>;
        |               schema:isPartOf ?projectId;   
        |               schema:url ?url;
        |               schema:name ?name;
        |               schema:alternateName ?alternateName;
        |               renku:topmostSameAs ?topmostSameAs;
        |               renku:topmostDerivedFrom/schema:identifier ?initialVersion .
        |    OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
        |    OPTIONAL { ?datasetId schema:description ?description }.
        |    OPTIONAL { ?datasetId schema:dateCreated ?maybeDateCreated }.
        |    OPTIONAL { ?datasetId schema:datePublished ?maybePublishedDate }.
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

  def findImages(identifier: Identifier): IO[List[ImageUri]] =
    queryExpecting[List[ImageUri]](using = queryImages(identifier))

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

  private[rest] def datasetsDecoder(usedIns: List[DatasetProject]): Decoder[List[Dataset]] = {
    val dataset: Decoder[Dataset] = { implicit cursor =>
      for {
        identifier         <- extract[Identifier]("identifier")
        title              <- extract[Title]("name")
        name               <- extract[Name]("alternateName")
        url                <- extract[Url]("url")
        maybeDerivedFrom   <- extract[Option[DerivedFrom]]("maybeDerivedFrom")
        sameAs             <- extract[SameAs]("topmostSameAs")
        initialVersion     <- extract[InitialVersion]("initialVersion")
        maybePublishedDate <- extract[Option[PublishedDate]]("maybePublishedDate")
        maybeDateCreated   <- extract[Option[DateCreated]]("maybeDateCreated")
        maybeDescription <- extract[Option[String]]("description")
                              .map(blankToNone)
                              .flatMap(toOption[Description])
        dates <- Dates
                   .from(maybeDateCreated, maybePublishedDate)
                   .leftMap(e => DecodingFailure(e.getMessage, Nil))
        path <-
          extract[projects.ResourceId]("projectId")
            .flatMap(toProjectPath)
        project <- Either.fromOption(usedIns.find(_.path == path),
                                     ifNone = DecodingFailure("Could not find project in UsedIns", Nil)
                   )
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
            creators = Set.empty,
            dates = dates,
            parts = List.empty,
            project = project,
            usedIn = List.empty,
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
            creators = Set.empty,
            dates = dates,
            parts = List.empty,
            project = project,
            usedIn = List.empty,
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

  private implicit lazy val imagesDecoder: Decoder[List[ImageUri]] = {

    implicit val imageDecoder: Decoder[ImageUri] =
      _.downField("contentUrl").downField("value").as[String].map(ImageUri.apply)

    _.downField("results").downField("bindings").as(decodeList[ImageUri])
  }

  def toProjectPath(projectPath: ResourceId) =
    projectPath
      .as[Try, Path]
      .toEither
      .leftMap(ex => DecodingFailure(ex.getMessage, Nil))

  private def extract[T](property: String)(implicit cursor: HCursor, decoder: Decoder[T]): Result[T] =
    cursor.downField(property).downField("value").as[T]
}

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

import cats.effect.{ConcurrentEffect, Timer}
import cats.syntax.all._
import ch.datascience.graph.model.datasets.{Identifier, ImageUri, Keyword}
import ch.datascience.graph.model.projects
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.knowledgegraph.datasets.model.Dataset
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import eu.timepit.refined.auto._
import io.circe.{DecodingFailure, HCursor}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.util.Try

private class BaseDetailsFinder[Interpretation[_]: ConcurrentEffect: Timer](
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[Interpretation],
    timeRecorder:            SparqlQueryTimeRecorder[Interpretation]
)(implicit executionContext: ExecutionContext)
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder) {

  import BaseDetailsFinder._
  import ch.datascience.graph.model.Schemas._

  def findBaseDetails(identifier: Identifier): Interpretation[Option[Dataset]] =
    queryExpecting[List[Dataset]](using = queryForDatasetDetails(identifier)) >>= toSingleDataset

  private def queryForDatasetDetails(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - details",
    Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?identifier ?name ?maybeDateCreated ?alternateName ?url ?topmostSameAs ?maybeDerivedFrom ?initialVersion ?description ?maybeDatePublished ?projectId ?projectName
        |WHERE {        
        |  {
        |    SELECT ?projectId ?projectName
        |    WHERE {
        |      ?datasetId a schema:Dataset;
        |                 schema:identifier '$identifier';
        |                 schema:isPartOf ?projectId .
        |      ?projectId schema:dateCreated ?dateCreated ;
        |                 schema:name ?projectName .
        |      FILTER NOT EXISTS {
        |        ?datasetId prov:invalidatedAtTime ?invalidationTime.
        |      }
        |      FILTER NOT EXISTS {
        |        ?parentDsId prov:wasDerivedFrom / schema:url ?datasetId.
        |        ?parentDsId prov:invalidatedAtTime ?invalidationTimeOnChild;
        |                    schema:isPartOf ?projectId.
        |      }
        |    }
        |    ORDER BY ?dateCreated ?projectName
        |    LIMIT 1
        |  }
        |  
        |  ?datasetId schema:identifier '$identifier';
        |             schema:identifier ?identifier;
        |             a schema:Dataset;
        |             schema:isPartOf ?projectId;   
        |             schema:url ?url;
        |             schema:name ?name;
        |             schema:alternateName ?alternateName;
        |             renku:topmostSameAs ?topmostSameAs;
        |             renku:topmostDerivedFrom/schema:identifier ?initialVersion .
        |  OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
        |  OPTIONAL { ?datasetId schema:description ?description }.
        |  OPTIONAL { ?datasetId schema:dateCreated ?maybeDateCreated }.
        |  OPTIONAL { ?datasetId schema:datePublished ?maybeDatePublished }.
        |}
        |""".stripMargin
  )

  def findKeywords(identifier: Identifier): Interpretation[List[Keyword]] =
    queryExpecting[List[Keyword]](using = queryKeywords(identifier))

  private def queryKeywords(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - keyword details",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?keyword
        |WHERE {
        |  ?datasetId schema:identifier "$identifier" ;
        |             schema:keywords ?keyword .
        |}
        |ORDER BY ASC(?keyword)
        |""".stripMargin
  )

  def findImages(identifier: Identifier): Interpretation[List[ImageUri]] =
    queryExpecting[List[ImageUri]](using = queryImages(identifier))

  private def queryImages(identifier: Identifier) = SparqlQuery.of(
    name = "ds by id - image urls",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?contentUrl
        |WHERE {
        |    ?datasetId schema:identifier "$identifier" ;
        |               schema:image ?imageId .
        |    ?imageId a schema:ImageObject;
        |             schema:contentUrl ?contentUrl ;
        |             schema:position ?position .
        |}ORDER BY ASC(?position)
        |""".stripMargin
  )

  private lazy val toSingleDataset: List[Dataset] => Interpretation[Option[Dataset]] = {
    case Nil            => Option.empty[Dataset].pure[Interpretation]
    case dataset :: Nil => Option(dataset).pure[Interpretation]
    case dataset :: _ =>
      new Exception(
        s"More than one dataset with ${dataset.id} id"
      ).raiseError[Interpretation, Option[Dataset]]
  }
}

private object BaseDetailsFinder {

  import io.circe.Decoder
  import Decoder._
  import ch.datascience.graph.model.datasets._
  import ch.datascience.knowledgegraph.datasets.model._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._

  private lazy val createDataset: (Identifier,
                                   Title,
                                   Name,
                                   Url,
                                   Option[DerivedFrom],
                                   SameAs,
                                   InitialVersion,
                                   Date,
                                   Option[Description],
                                   DatasetProject
  ) => Result[Dataset] = {
    case (id, title, name, url, Some(derived), _, initialVersion, dates: DateCreated, maybeDesc, project) =>
      ModifiedDataset(
        id,
        title,
        name,
        url,
        derived,
        DatasetVersions(initialVersion),
        maybeDesc,
        creators = Set.empty,
        date = dates,
        parts = List.empty,
        project = project,
        usedIn = List.empty,
        keywords = List.empty,
        images = List.empty
      ).asRight[DecodingFailure]
    case (identifier, title, name, url, None, sameAs, initialVersion, date, maybeDescription, project) =>
      NonModifiedDataset(
        identifier,
        title,
        name,
        url,
        sameAs,
        DatasetVersions(initialVersion),
        maybeDescription,
        creators = Set.empty,
        date = date,
        parts = List.empty,
        project = project,
        usedIn = List.empty,
        keywords = List.empty,
        images = List.empty
      ).asRight[DecodingFailure]
    case (identifier, title, _, _, _, _, _, _, _, _) =>
      DecodingFailure(
        s"'$title' dataset with id '$identifier' does not meet validation for modified nor non-modified dataset",
        Nil
      ).asLeft[Dataset]
  }

  private[rest] implicit lazy val datasetsDecoder: Decoder[List[Dataset]] = {
    val dataset: Decoder[Dataset] = { implicit cursor =>
      for {
        identifier       <- extract[Identifier]("identifier")
        title            <- extract[Title]("name")
        name             <- extract[Name]("alternateName")
        url              <- extract[Url]("url")
        maybeDerivedFrom <- extract[Option[DerivedFrom]]("maybeDerivedFrom")
        sameAs           <- extract[SameAs]("topmostSameAs")
        initialVersion   <- extract[InitialVersion]("initialVersion")
        date <- maybeDerivedFrom match {
                  case Some(_) => extract[DateCreated]("maybeDateCreated").widen[Date]
                  case _ =>
                    extract[Option[DatePublished]]("maybeDatePublished")
                      .flatMap {
                        case Some(published) => published.asRight
                        case None            => extract[DateCreated]("maybeDateCreated")
                      }
                      .widen[Date]
                }
        maybeDescription <- extract[Option[String]]("description")
                              .map(blankToNone)
                              .flatMap(toOption[Description])
        projectPath <- extract[projects.ResourceId]("projectId").flatMap(toProjectPath)
        projectName <- extract[projects.Name]("projectName")
        dataset <- createDataset(identifier,
                                 title,
                                 name,
                                 url,
                                 maybeDerivedFrom,
                                 sameAs,
                                 initialVersion,
                                 date,
                                 maybeDescription,
                                 DatasetProject(projectPath, projectName)
                   )
      } yield dataset
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

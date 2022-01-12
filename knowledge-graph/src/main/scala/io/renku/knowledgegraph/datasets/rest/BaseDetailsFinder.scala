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

package io.renku.knowledgegraph.datasets.rest

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{DecodingFailure, HCursor}
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model.datasets.{Identifier, ImageUri, Keyword}
import io.renku.graph.model.projects
import io.renku.graph.model.projects.Path
import io.renku.knowledgegraph.datasets.model.Dataset
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait BaseDetailsFinder[F[_]] {
  def findBaseDetails(identifier: Identifier, authContext: AuthContext[Identifier]): F[Option[Dataset]]
  def findKeywords(identifier:    Identifier): F[List[Keyword]]
  def findImages(identifier:      Identifier): F[List[ImageUri]]
}

private class BaseDetailsFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with BaseDetailsFinder[F] {

  import BaseDetailsFinderImpl._
  import io.renku.graph.model.Schemas._

  def findBaseDetails(identifier: Identifier, authContext: AuthContext[Identifier]): F[Option[Dataset]] =
    queryExpecting[List[Dataset]](using = queryForDatasetDetails(identifier, authContext)) >>= toSingleDataset

  private def queryForDatasetDetails(identifier: Identifier, authContext: AuthContext[Identifier]) = SparqlQuery.of(
    name = "ds by id - details",
    Prefixes.of(prov -> "prov", renku -> "renku", schema -> "schema"),
    s"""|SELECT DISTINCT ?datasetId ?identifier ?name ?maybeDateCreated ?slug ?topmostSameAs ?maybeDerivedFrom ?initialVersion ?description ?maybeDatePublished ?projectPath ?projectName
        |WHERE {        
        |  {
        |    SELECT ?projectId ?projectPath ?projectName
        |    WHERE {
        |      ?datasetId a schema:Dataset;
        |                 schema:identifier '$identifier';
        |                 ^renku:hasDataset  ?projectId.
        |      ?projectId schema:dateCreated ?dateCreated ;
        |                 schema:name ?projectName;
        |                 renku:projectPath ?projectPath.
        |      FILTER (?projectPath IN (${authContext.allowedProjects.map(p => s"'$p'").mkString(", ")})) 
        |      FILTER NOT EXISTS {
        |        ?datasetId prov:invalidatedAtTime ?invalidationTime.
        |      }
        |      FILTER NOT EXISTS {
        |        ?parentDsId prov:wasDerivedFrom / schema:url ?datasetId.
        |        ?parentDsId prov:invalidatedAtTime ?invalidationTimeOnChild;
        |                    ^renku:hasDataset  ?projectId.
        |      }
        |    }
        |    ORDER BY ?dateCreated ?projectName
        |    LIMIT 1
        |  }
        |  
        |  ?datasetId schema:identifier '$identifier';
        |             schema:identifier ?identifier;
        |             a schema:Dataset;
        |             ^renku:hasDataset  ?projectId;   
        |             schema:name ?name;
        |             renku:slug ?slug;
        |             renku:topmostSameAs ?topmostSameAs;
        |             renku:topmostDerivedFrom/schema:identifier ?initialVersion .
        |  OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }.
        |  OPTIONAL { ?datasetId schema:description ?description }.
        |  OPTIONAL { ?datasetId schema:dateCreated ?maybeDateCreated }.
        |  OPTIONAL { ?datasetId schema:datePublished ?maybeDatePublished }.
        |}
        |""".stripMargin
  )

  def findKeywords(identifier: Identifier): F[List[Keyword]] =
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

  def findImages(identifier: Identifier): F[List[ImageUri]] =
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

  private lazy val toSingleDataset: List[Dataset] => F[Option[Dataset]] = {
    case Nil            => Option.empty[Dataset].pure[F]
    case dataset :: Nil => Option(dataset).pure[F]
    case dataset :: _ =>
      new Exception(s"More than one dataset with ${dataset.id} id").raiseError[F, Option[Dataset]]
  }
}

private object BaseDetailsFinder {

  def apply[F[_]: Async: Logger](rdfStoreConfig: RdfStoreConfig,
                                 timeRecorder: SparqlQueryTimeRecorder[F]
  ): F[BaseDetailsFinder[F]] =
    MonadThrow[F].catchNonFatal(new BaseDetailsFinderImpl[F](rdfStoreConfig, timeRecorder))
}

private object BaseDetailsFinderImpl {

  import io.circe.Decoder
  import Decoder._
  import io.renku.graph.model.datasets._
  import io.renku.knowledgegraph.datasets.model._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  private lazy val createDataset: (ResourceId,
                                   Identifier,
                                   Title,
                                   Name,
                                   Option[DerivedFrom],
                                   SameAs,
                                   InitialVersion,
                                   Date,
                                   Option[Description],
                                   DatasetProject
  ) => Result[Dataset] = {
    case (resourceId, id, title, name, Some(derived), _, initialVersion, dates: DateCreated, maybeDesc, project) =>
      ModifiedDataset(
        resourceId,
        id,
        title,
        name,
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
    case (resourceId, identifier, title, name, None, sameAs, initialVersion, date, maybeDescription, project) =>
      NonModifiedDataset(
        resourceId,
        identifier,
        title,
        name,
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
        resourceId       <- extract[ResourceId]("datasetId")
        identifier       <- extract[Identifier]("identifier")
        title            <- extract[Title]("name")
        name             <- extract[Name]("slug")
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
        maybeDescription <- extract[Option[Description]]("description")
        projectPath      <- extract[projects.Path]("projectPath")
        projectName      <- extract[projects.Name]("projectName")
        dataset <- createDataset(resourceId,
                                 identifier,
                                 title,
                                 name,
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

  private def extract[T](property: String)(implicit cursor: HCursor, decoder: Decoder[T]): Result[T] =
    cursor.downField(property).downField("value").as[T]
}

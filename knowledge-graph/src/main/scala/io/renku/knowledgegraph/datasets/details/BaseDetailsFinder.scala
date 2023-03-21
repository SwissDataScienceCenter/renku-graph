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
package details

import Dataset.Tag
import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.{Decoder, DecodingFailure}
import io.renku.graph.http.server.security.Authorizer.AuthContext
import io.renku.graph.model._
import io.renku.graph.model.datasets.{Identifier, Keyword, SameAs}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.ImageUri
import io.renku.graph.model.projects.Path
import io.renku.http.server.security.model.AuthUser
import io.renku.jsonld.syntax._
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.sparql.Fragment
import org.typelevel.log4cats.Logger

private trait BaseDetailsFinder[F[_]] {
  def findBaseDetails(identifier: RequestedDataset, authContext: AuthContext[RequestedDataset]): F[Option[Dataset]]
  def findInitialTag(dataset:     Dataset, authContext:          AuthContext[RequestedDataset]): F[Option[Tag]]
  def findKeywords(dataset:       Dataset): F[List[Keyword]]
  def findImages(dataset:         Dataset): F[List[ImageUri]]
}

private class BaseDetailsFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClientImpl(storeConfig)
    with BaseDetailsFinder[F] {

  import BaseDetailsFinderImpl._
  import io.renku.graph.model.Schemas._

  def findBaseDetails(identifier: RequestedDataset, authContext: AuthContext[RequestedDataset]): F[Option[Dataset]] = {
    implicit val decoder: Decoder[Option[Dataset]] = maybeDatasetDecoder(identifier)
    queryExpecting[Option[Dataset]](selectQuery =
      identifier.fold(queryWithIdentifier(_, authContext), queryWithSameAs(_, authContext))
    )
  }

  private def queryWithIdentifier(identifier: Identifier, authContext: AuthContext[RequestedDataset]) =
    SparqlQuery.of(
      name = "ds by id - details",
      Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?datasetId ?identifier ?name ?maybeDateCreated ?slug
               |  ?topmostSameAs ?maybeDerivedFrom ?initialVersion ?description ?maybeDatePublished 
               |  ?projectId ?projectPath ?projectName ?projectVisibility
               |WHERE {        
               |  {
               |    SELECT ?projectId ?projectPath ?projectName ?projectVisibility
               |    WHERE {
               |      GRAPH ?projectId {
               |        ?datasetId a schema:Dataset;
               |                   schema:identifier ${identifier.asObject};
               |                   ^renku:hasDataset  ?projectId.
               |        ?projectId schema:dateCreated ?dateCreated ;
               |                   schema:name ?projectName;
               |                   renku:projectVisibility ?projectVisibility;
               |                   renku:projectPath ?projectPath.
               |        VALUES (?projectPath) { ${authContext.allowedProjects.map(_.asObject)} }
               |        FILTER NOT EXISTS {
               |          ?datasetId prov:invalidatedAtTime ?invalidationTime.
               |        }
               |        FILTER NOT EXISTS {
               |          ?parentDsId prov:wasDerivedFrom / schema:url ?datasetId.
               |          ?parentDsId prov:invalidatedAtTime ?invalidationTimeOnChild;
               |                      ^renku:hasDataset  ?projectId.
               |        }
               |      }
               |    }
               |    ORDER BY ?dateCreated ?projectName
               |    LIMIT 1
               |  }
               |
               |  GRAPH ?projectId {
               |    ?datasetId schema:identifier ${identifier.asObject};
               |               schema:identifier ?identifier;
               |               a schema:Dataset;
               |               ^renku:hasDataset ?projectId;
               |               schema:name ?name;
               |               renku:slug ?slug;
               |               renku:topmostSameAs ?topmostSameAs;
               |               renku:topmostDerivedFrom/schema:identifier ?initialVersion .
               |    OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }
               |    OPTIONAL { ?datasetId schema:description ?description }
               |    OPTIONAL { ?datasetId schema:dateCreated ?maybeDateCreated }
               |    OPTIONAL { ?datasetId schema:datePublished ?maybeDatePublished }
               |  }
               |}
               |""".stripMargin.sparql
    )

  private def queryWithSameAs(sameAs: SameAs, authContext: AuthContext[RequestedDataset]) =
    SparqlQuery.of(
      name = "ds by sameAs - details",
      Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?datasetId ?identifier ?name ?maybeDateCreated ?slug
               |  ?topmostSameAs ?maybeDerivedFrom ?initialVersion ?description ?maybeDatePublished
               |  ?projectId ?projectPath ?projectName ?projectVisibility
               |WHERE {
               |  {
               |    SELECT ?datasetId ?projectId ?projectPath ?projectName ?projectVisibility
               |    WHERE {
               |      GRAPH ${GraphClass.Datasets.id} {
               |        BIND (${datasets.TopmostSameAs(sameAs).asEntityId} AS ?topmostSameAs)
               |        ?topmostSameAs a renku:DiscoverableDataset;
               |                       renku:datasetProjectLink ?dsProjLink.
               |        ?dsProjLink renku:project ?projectId;
               |                    renku:dataset ?datasetId.
               |      }
               |      GRAPH ?projectId {
               |        ?projectId schema:dateCreated ?dateCreated ;
               |                   schema:name ?projectName;
               |                   renku:projectVisibility ?projectVisibility;
               |                   renku:projectPath ?projectPath.
               |        VALUES (?projectPath) { ${authContext.allowedProjects.map(_.asObject)} }
               |      }
               |    }
               |    ORDER BY ?dateCreated ?projectName
               |    LIMIT 1
               |  }
               |
               |  GRAPH ?projectId {
               |    ?datasetId renku:topmostSameAs ?topmostSameAs;
               |               schema:identifier ?identifier;
               |               ^renku:hasDataset ?projectId;
               |               schema:name ?name;
               |               renku:slug ?slug;
               |               renku:topmostDerivedFrom/schema:identifier ?initialVersion .
               |    OPTIONAL { ?datasetId prov:wasDerivedFrom/schema:url ?maybeDerivedFrom }
               |    OPTIONAL { ?datasetId schema:description ?description }
               |    OPTIONAL { ?datasetId schema:dateCreated ?maybeDateCreated }
               |    OPTIONAL { ?datasetId schema:datePublished ?maybeDatePublished }
               |  }
               |}
               |""".stripMargin.sparql
    )

  def findInitialTag(dataset: Dataset, authContext: AuthContext[RequestedDataset]): F[Option[Tag]] =
    queryExpecting[Option[Tag]](queryInitialTag(dataset, authContext))

  private def queryInitialTag(ds: Dataset, authContext: AuthContext[RequestedDataset]) = SparqlQuery.of(
    name = "ds by id - initial tag",
    Prefixes of (renku -> "renku", schema -> "schema"),
    sparql"""|SELECT DISTINCT ?tagName ?maybeTagDesc
             |WHERE {
             |  GRAPH ${GraphClass.Project.id(ds.project.id)} {
             |    ?datasetId schema:identifier ${ds.id.asObject};
             |               schema:version ?version;
             |               schema:sameAs/schema:url ?originalDsId
             |  }
             |  GRAPH ?originalDsProjId {
             |    ?originalDsProjId renku:hasDataset ?originalDsId;
             |                      renku:projectPath ?projectPath.
             |    ${allowedProjectFilterQuery(authContext.maybeAuthUser)}
             |    ?originalDsTagId schema:about/schema:url ?originalDsId;
             |                     schema:name ?version
             |  }
             |  GRAPH ${GraphClass.Project.id(ds.project.id)} {
             |    ?datasetTagId schema:about/schema:url ?datasetId;
             |                  schema:name ?version.
             |    BIND (?version AS ?tagName)
             |    OPTIONAL { ?datasetTagId schema:description ?maybeTagDesc }
             |  }
             |}
             |LIMIT 1
             |""".stripMargin.sparql
  )

  private lazy val allowedProjectFilterQuery: Option[AuthUser] => Fragment = {
    case Some(user) =>
      fr"""|?originalDsProjId renku:projectVisibility ?visibility .
           |OPTIONAL {
           |  ?originalDsProjId schema:member ?memberId
           |  GRAPH ${GraphClass.Persons.id} {
           |    ?memberId schema:sameAs ?sameAsId.
           |    ?sameAsId schema:additionalType ${Person.gitLabSameAsAdditionalType.asTripleObject};
           |              schema:identifier ?userGitlabId
           |  }
           |}
           |FILTER (?visibility = ${projects.Visibility.Public.asObject} || ?userGitlabId = ${user.id.asObject})
           |""".stripMargin
    case _ =>
      fr"""|?originalDsProjId renku:projectVisibility ?visibility .
           |VALUES (?visibility) { (${projects.Visibility.Public.asObject})}
           |""".stripMargin
  }

  def findKeywords(dataset: Dataset): F[List[Keyword]] =
    queryExpecting[List[Keyword]](queryKeywords(dataset))

  private def queryKeywords(ds: Dataset) = SparqlQuery.of(
    name = "ds by id - keywords",
    Prefixes of schema -> "schema",
    sparql"""|SELECT DISTINCT ?keyword
             |FROM ${GraphClass.Project.id(ds.project.id)} {
             |  ?datasetId schema:identifier ${ds.id.asObject};
             |             schema:keywords ?keyword
             |}
             |ORDER BY ASC(?keyword)
             |""".stripMargin.sparql
  )

  def findImages(dataset: Dataset): F[List[ImageUri]] =
    queryExpecting[List[ImageUri]](queryImages(dataset))

  private def queryImages(ds: Dataset) = SparqlQuery.of(
    name = "ds by id - image urls",
    Prefixes of schema -> "schema",
    sparql"""|SELECT DISTINCT ?contentUrl
             |FROM ${GraphClass.Project.id(ds.project.id)} {
             |  ?datasetId schema:identifier ${ds.id.asObject};
             |             schema:image ?imageId .
             |  ?imageId a schema:ImageObject;
             |           schema:contentUrl ?contentUrl ;
             |           schema:position ?position
             |}
             |ORDER BY ASC(?position)
             |""".stripMargin.sparql
  )
}

private object BaseDetailsFinder {

  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder](
      storeConfig: ProjectsConnectionConfig
  ): F[BaseDetailsFinder[F]] = MonadThrow[F].catchNonFatal(new BaseDetailsFinderImpl[F](storeConfig))
}

private object BaseDetailsFinderImpl {

  import Dataset._
  import io.circe.Decoder
  import Decoder._
  import io.renku.graph.model.datasets._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import io.renku.triplesstore.ResultsDecoder._

  private lazy val createDataset: (ResourceId,
                                   Identifier,
                                   Title,
                                   Name,
                                   Option[DerivedFrom],
                                   SameAs,
                                   OriginalIdentifier,
                                   CreatedOrPublished,
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
        maybeInitialTag = None,
        maybeDesc,
        creators = List.empty,
        createdOrPublished = dates,
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
        maybeInitialTag = None,
        maybeDescription,
        creators = List.empty,
        createdOrPublished = date,
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

  private[datasets] def maybeDatasetDecoder(identifier: RequestedDataset): Decoder[Option[Dataset]] =
    ResultsDecoder[Option, Dataset] { implicit cursor =>
      for {
        resourceId       <- extract[ResourceId]("datasetId")
        identifier       <- extract[Identifier]("identifier")
        title            <- extract[Title]("name")
        name             <- extract[Name]("slug")
        maybeDerivedFrom <- extract[Option[DerivedFrom]]("maybeDerivedFrom")
        sameAs           <- extract[SameAs]("topmostSameAs")
        initialVersion   <- extract[OriginalIdentifier]("initialVersion")
        createdOrPublished <- maybeDerivedFrom match {
                                case Some(_) => extract[DateCreated]("maybeDateCreated").widen[CreatedOrPublished]
                                case _ =>
                                  extract[Option[DatePublished]]("maybeDatePublished")
                                    .flatMap {
                                      case Some(published) => published.asRight
                                      case None            => extract[DateCreated]("maybeDateCreated")
                                    }
                                    .widen[CreatedOrPublished]
                              }
        maybeDescription <- extract[Option[Description]]("description")

        project <- (extract[projects.ResourceId]("projectId"),
                    extract[projects.Path]("projectPath"),
                    extract[projects.Name]("projectName"),
                    extract[projects.Visibility]("projectVisibility")
                   ).mapN(DatasetProject)

        dataset <- createDataset(resourceId,
                                 identifier,
                                 title,
                                 name,
                                 maybeDerivedFrom,
                                 sameAs,
                                 initialVersion,
                                 createdOrPublished,
                                 maybeDescription,
                                 project
                   )
      } yield dataset
    }(toOption(show"More than one dataset with $identifier id"))

  private implicit lazy val maybeInitialTagDecoder: Decoder[Option[Tag]] = ResultsDecoder[Option, Tag] { implicit cur =>
    (extract[publicationEvents.Name]("tagName") -> extract[Option[publicationEvents.Description]]("maybeTagDesc"))
      .mapN((name, maybeDesc) => Tag(name, maybeDesc))
  }

  private implicit lazy val keywordsDecoder: Decoder[List[Keyword]] =
    ResultsDecoder[List, Keyword](implicit cur => extract("keyword"))

  private implicit lazy val imagesDecoder: Decoder[List[ImageUri]] =
    ResultsDecoder[List, ImageUri](implicit cur => extract("contentUrl"))
}

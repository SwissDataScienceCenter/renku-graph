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

package io.renku.triplesgenerator.tsprovisioning.transformation.namedgraphs.datasets

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model._
import io.renku.triplesstore.ResultsDecoder._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait KGDatasetInfoFinder[F[_]] {

  def findParentTopmostSameAs(idSameAs: datasets.InternalSameAs): F[Option[datasets.TopmostSameAs]]
  def findTopmostSameAs(projectId:      projects.ResourceId, dsId: datasets.ResourceId): F[Set[datasets.TopmostSameAs]]
  def findDatasetCreators(projectId:    projects.ResourceId, dsId: datasets.ResourceId): F[Set[persons.ResourceId]]
  def findDatasetOriginalIdentifiers(projectId: projects.ResourceId,
                                     dsId:      datasets.ResourceId
  ): F[Set[datasets.OriginalIdentifier]]
  def findDatasetDateCreated(projectId:  projects.ResourceId, dsId: datasets.ResourceId): F[Set[datasets.DateCreated]]
  def findDatasetDescriptions(projectId: projects.ResourceId, dsId: datasets.ResourceId): F[Set[datasets.Description]]
  def findDatasetSameAs(projectId:       projects.ResourceId, dsId: datasets.ResourceId): F[Set[datasets.SameAs]]
  def findWhereNotInvalidated(dsId:      datasets.ResourceId): F[Set[projects.ResourceId]]
  def checkPublicationEventsExist(projectId: projects.ResourceId, dsId: datasets.ResourceId): F[Boolean]
}

private class KGDatasetInfoFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    connectionConfig: ProjectsConnectionConfig
) extends TSClientImpl(connectionConfig)
    with KGDatasetInfoFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import io.renku.triplesstore.client.syntax._

  override def findParentTopmostSameAs(sameAs: datasets.InternalSameAs): F[Option[datasets.TopmostSameAs]] = {
    implicit val decoder: Decoder[Option[datasets.TopmostSameAs]] = ResultsDecoder[Option, datasets.TopmostSameAs] {
      implicit cur => extract[datasets.TopmostSameAs]("topmostSameAs")
    }(toOption(onMultiple = show"Multiple topmostSameAs found for dataset ${sameAs.show}"))

    queryExpecting[Option[datasets.TopmostSameAs]](
      SparqlQuery.of(
        name = "transformation - find parent topmostSameAs",
        Prefixes of (renku -> "renku", schema -> "schema", xsd -> "xsd"),
        sparql"""|SELECT ?topmostSameAs
                 |WHERE {
                 |  GRAPH ?g {
                 |    ${sameAs.asResourceId.asEntityId} a schema:Dataset;
                 |                                      renku:topmostSameAs ?topmostSameAs;
                 |                                      ^renku:hasDataset ?projectId.
                 |    ?projectId schema:dateCreated ?projectDateCreated
                 |  }
                 |}
                 |ORDER BY ASC(xsd:date(?projectDateCreated))
                 |LIMIT 1
                 |""".stripMargin
      )
    )
  }

  override def findTopmostSameAs(projectId: projects.ResourceId,
                                 dsId:      datasets.ResourceId
  ): F[Set[datasets.TopmostSameAs]] = {
    implicit val decoder: Decoder[Set[datasets.TopmostSameAs]] = ResultsDecoder[Set, datasets.TopmostSameAs] {
      implicit cur => extract[datasets.TopmostSameAs]("topmostSameAs")
    }

    queryExpecting[Set[datasets.TopmostSameAs]](
      SparqlQuery.of(
        name = "transformation - find topmostSameAs",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?topmostSameAs
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    ${dsId.asEntityId} a schema:Dataset;
                 |                       renku:topmostSameAs ?topmostSameAs.
                 |  }
                 |}
                 |""".stripMargin
      )
    )
  }

  override def findDatasetCreators(projectId: projects.ResourceId,
                                   dsId:      datasets.ResourceId
  ): F[Set[persons.ResourceId]] = {
    implicit val creatorsDecoder: Decoder[Set[persons.ResourceId]] = ResultsDecoder[Set, persons.ResourceId] {
      implicit cur => extract[persons.ResourceId]("personId")
    }

    queryExpecting[Set[persons.ResourceId]] {
      SparqlQuery.of(
        name = "transformation - find ds creators",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?personId
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    ${dsId.asEntityId} a schema:Dataset;
                 |                       schema:creator ?personId
                 |  }
                 |}
                 |""".stripMargin
      )
    }
  }

  override def findDatasetOriginalIdentifiers(projectId: projects.ResourceId,
                                              dsId:      datasets.ResourceId
  ): F[Set[datasets.OriginalIdentifier]] = {
    implicit val decoder: Decoder[Set[datasets.OriginalIdentifier]] = ResultsDecoder[Set, datasets.OriginalIdentifier] {
      implicit cursor => extract("originalId")
    }
    queryExpecting[Set[datasets.OriginalIdentifier]] {
      SparqlQuery.of(
        name = "transformation - find ds originalIdentifiers",
        Prefixes of (renku -> "renku", schema -> "schema"),
        sparql"""|SELECT ?originalId
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    ${dsId.asEntityId} a schema:Dataset;
                 |                       renku:originalIdentifier ?originalId.
                 |  }
                 |}
                 |""".stripMargin
      )
    }
  }

  def findDatasetDateCreated(projectId: projects.ResourceId,
                             dsId:      datasets.ResourceId
  ): F[Set[datasets.DateCreated]] = {
    implicit val decoder: Decoder[Set[datasets.DateCreated]] = ResultsDecoder[Set, datasets.DateCreated] {
      implicit cursor => extract("date")
    }
    queryExpecting[Set[datasets.DateCreated]] {
      SparqlQuery.of(
        name = "transformation - find ds originalIdentifiers",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?date
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    ${dsId.asEntityId} a schema:Dataset;
                 |                       schema:dateCreated ?date.
                 |  }
                 |}
                 |""".stripMargin
      )
    }
  }

  override def findDatasetDescriptions(projectId: projects.ResourceId,
                                       dsId:      datasets.ResourceId
  ): F[Set[datasets.Description]] = {
    implicit val decoder: Decoder[Set[datasets.Description]] = ResultsDecoder[Set, datasets.Description] {
      implicit cursor => extract("desc")
    }
    queryExpecting[Set[datasets.Description]] {
      SparqlQuery.of(
        name = "transformation - find ds descriptions",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?desc
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    ${dsId.asEntityId} a schema:Dataset;
                 |                       schema:description ?desc.
                 |  }
                 |}
                 |""".stripMargin
      )
    }
  }

  override def findDatasetSameAs(projectId: projects.ResourceId, dsId: datasets.ResourceId): F[Set[datasets.SameAs]] = {
    implicit val decoder: Decoder[Set[datasets.SameAs]] = ResultsDecoder[Set, datasets.SameAs] { implicit cursor =>
      extract("sameAs")
    }
    queryExpecting[Set[datasets.SameAs]] {
      SparqlQuery.of(
        name = "transformation - find ds sameAs",
        Prefixes of schema -> "schema",
        sparql"""|SELECT ?sameAs
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    ${dsId.asEntityId} a schema:Dataset;
                 |                       schema:sameAs ?sameAs.
                 |  }
                 |}
                 |""".stripMargin
      )
    }
  }

  override def findWhereNotInvalidated(dsId: datasets.ResourceId): F[Set[projects.ResourceId]] = {
    implicit val decoder: Decoder[Set[projects.ResourceId]] = ResultsDecoder[Set, projects.ResourceId] {
      implicit cursor => extract("projectId")
    }
    queryExpecting[Set[projects.ResourceId]] {
      SparqlQuery.of(
        name = "transformation - find where DS not invalidated",
        Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
        sparql"""|SELECT DISTINCT ?projectId
                 |WHERE {
                 |  GRAPH ?projectId {
                 |    BIND (${dsId.asEntityId} AS ?invalidatedDS)
                 |    ?invalidatedDS a schema:Dataset;
                 |                   ^renku:hasDataset ?projectId.
                 |    FILTER NOT EXISTS {
                 |      ?invalidationDS prov:wasDerivedFrom/schema:url ?invalidatedDS;
                 |                      prov:invalidatedAtTime ?invalidationTime.
                 |    }
                 |  }
                 |}
                 |""".stripMargin
      )
    }
  }

  override def checkPublicationEventsExist(projectId: projects.ResourceId, dsId: datasets.ResourceId): F[Boolean] = {

    implicit val decoder: Decoder[Boolean] = ResultsDecoder
      .single[Int](implicit cursor => extract[Int]("idsCount"))
      .map(_ > 0)

    queryExpecting[Boolean] {
      SparqlQuery.of(
        name = "transformation - check publicationEvents exist",
        Prefixes of schema -> "schema",
        sparql"""|SELECT (COUNT(?peId) AS ?idsCount)
                 |WHERE {
                 |  GRAPH ${GraphClass.Project.id(projectId)} {
                 |    ?peId a schema:PublicationEvent;
                 |          schema:about/schema:url ${dsId.asEntityId}
                 |  }
                 |}
                 |""".stripMargin
      )
    }
  }
}

private object KGDatasetInfoFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGDatasetInfoFinder[F]] =
    ProjectsConnectionConfig[F]().map(new KGDatasetInfoFinderImpl(_))
}

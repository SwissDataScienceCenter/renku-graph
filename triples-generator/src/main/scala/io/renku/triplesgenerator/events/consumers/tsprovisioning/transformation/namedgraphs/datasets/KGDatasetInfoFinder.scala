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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.datasets

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model.datasets.{DateCreated, Description, InternalSameAs, OriginalIdentifier, ResourceId, SameAs, TopmostSameAs}
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{GraphClass, persons, projects}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait KGDatasetInfoFinder[F[_]] {
  def findParentTopmostSameAs(idSameAs:         InternalSameAs): F[Option[TopmostSameAs]]
  def findTopmostSameAs(projectId:              projects.ResourceId, resourceId: ResourceId): F[Set[TopmostSameAs]]
  def findDatasetCreators(projectId:            projects.ResourceId, resourceId: ResourceId): F[Set[persons.ResourceId]]
  def findDatasetOriginalIdentifiers(projectId: projects.ResourceId, resourceId: ResourceId): F[Set[OriginalIdentifier]]
  def findDatasetDateCreated(projectId:         projects.ResourceId, resourceId: ResourceId): F[Set[DateCreated]]
  def findDatasetDescriptions(projectId:        projects.ResourceId, resourceId: ResourceId): F[Set[Description]]
  def findDatasetSameAs(projectId:              projects.ResourceId, resourceId: ResourceId): F[Set[SameAs]]
  def findWhereNotInvalidated(resourceId:       ResourceId): F[Set[projects.ResourceId]]
}

private class KGDatasetInfoFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    connectionConfig: ProjectsConnectionConfig
) extends TSClientImpl(connectionConfig)
    with KGDatasetInfoFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas._
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def findParentTopmostSameAs(sameAs: InternalSameAs): F[Option[TopmostSameAs]] = {
    implicit val decoder: Decoder[Option[TopmostSameAs]] = ResultsDecoder[Option, TopmostSameAs] { implicit cur =>
      extract[TopmostSameAs]("topmostSameAs")
    }(toOption(onMultiple = show"Multiple topmostSameAs found for dataset ${sameAs.show}"))

    queryExpecting[Option[TopmostSameAs]](
      SparqlQuery.of(
        name = "transformation - find parent topmostSameAs",
        Prefixes of (renku -> "renku", schema -> "schema", xsd -> "xsd"),
        s"""|SELECT ?topmostSameAs
            |WHERE {
            |  GRAPH ?g {
            |    <$sameAs> a schema:Dataset;
            |                renku:topmostSameAs ?topmostSameAs;
            |                ^renku:hasDataset ?projectId.
            |    ?projectId schema:dateCreated ?projectDateCreated
            |  }
            |}
            |ORDER BY ASC(xsd:date(?projectDateCreated))
            |LIMIT 1
            |""".stripMargin
      )
    )
  }

  override def findTopmostSameAs(projectId: projects.ResourceId, resourceId: ResourceId): F[Set[TopmostSameAs]] = {
    implicit val decoder: Decoder[Set[TopmostSameAs]] = ResultsDecoder[Set, TopmostSameAs] { implicit cur =>
      extract[TopmostSameAs]("topmostSameAs")
    }

    queryExpecting[Set[TopmostSameAs]](
      SparqlQuery.of(
        name = "transformation - find topmostSameAs",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""|SELECT ?topmostSameAs
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    <$resourceId> a schema:Dataset;
            |                  renku:topmostSameAs ?topmostSameAs.
            |  }
            |}
            |""".stripMargin
      )
    )
  }

  override def findDatasetCreators(projectId:  projects.ResourceId,
                                   resourceId: ResourceId
  ): F[Set[persons.ResourceId]] = {
    implicit val creatorsDecoder: Decoder[Set[persons.ResourceId]] = ResultsDecoder[Set, persons.ResourceId] {
      implicit cur => extract[persons.ResourceId]("personId")
    }

    queryExpecting[Set[persons.ResourceId]] {
      SparqlQuery.of(
        name = "transformation - find ds creators",
        Prefixes of schema -> "schema",
        s"""|SELECT ?personId
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                      schema:creator ?personId
            |  }
            |}
            |""".stripMargin
      )
    }
  }

  override def findDatasetOriginalIdentifiers(projectId:  projects.ResourceId,
                                              resourceId: ResourceId
  ): F[Set[OriginalIdentifier]] = {
    implicit val decoder: Decoder[Set[OriginalIdentifier]] = ResultsDecoder[Set, OriginalIdentifier] {
      implicit cursor => extract("originalId")
    }
    queryExpecting[Set[OriginalIdentifier]] {
      SparqlQuery.of(
        name = "transformation - find ds originalIdentifiers",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""|SELECT ?originalId
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                      renku:originalIdentifier ?originalId.
            |  }
            |}
            |""".stripMargin
      )
    }
  }

  def findDatasetDateCreated(projectId: projects.ResourceId, resourceId: ResourceId): F[Set[DateCreated]] = {
    implicit val decoder: Decoder[Set[DateCreated]] = ResultsDecoder[Set, DateCreated] { implicit cursor =>
      extract("date")
    }
    queryExpecting[Set[DateCreated]] {
      SparqlQuery.of(
        name = "transformation - find ds originalIdentifiers",
        Prefixes of schema -> "schema",
        s"""|SELECT ?date
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                      schema:dateCreated ?date.
            |  }
            |}
            |""".stripMargin
      )
    }
  }

  override def findDatasetDescriptions(projectId: projects.ResourceId, resourceId: ResourceId): F[Set[Description]] = {
    implicit val decoder: Decoder[Set[Description]] = ResultsDecoder[Set, Description] { implicit cursor =>
      extract("desc")
    }
    queryExpecting[Set[Description]] {
      SparqlQuery.of(
        name = "transformation - find ds descriptions",
        Prefixes of schema -> "schema",
        s"""|SELECT ?desc
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                      schema:description ?desc.
            |  }
            |}
            |""".stripMargin
      )
    }
  }

  override def findDatasetSameAs(projectId: projects.ResourceId, resourceId: ResourceId): F[Set[SameAs]] = {
    implicit val decoder: Decoder[Set[SameAs]] = ResultsDecoder[Set, SameAs] { implicit cursor =>
      extract("sameAs")
    }
    queryExpecting[Set[SameAs]] {
      SparqlQuery.of(
        name = "transformation - find ds sameAs",
        Prefixes of schema -> "schema",
        s"""|SELECT ?sameAs
            |WHERE {
            |  GRAPH <${GraphClass.Project.id(projectId)}> {
            |    ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                      schema:sameAs ?sameAs.
            |  }
            |}
            |""".stripMargin
      )
    }
  }

  override def findWhereNotInvalidated(resourceId: ResourceId): F[Set[projects.ResourceId]] = {
    implicit val decoder: Decoder[Set[projects.ResourceId]] = ResultsDecoder[Set, projects.ResourceId] {
      implicit cursor => extract("projectId")
    }
    queryExpecting[Set[projects.ResourceId]] {
      SparqlQuery.of(
        name = "transformation - find where DS not invalidated",
        Prefixes of (prov -> "prov", renku -> "renku", schema -> "schema"),
        s"""|SELECT DISTINCT ?projectId
            |WHERE {
            |  GRAPH ?projectId {
            |    BIND (${resourceId.showAs[RdfResource]} AS ?invalidatedDS)
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
}

private object KGDatasetInfoFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGDatasetInfoFinder[F]] =
    ProjectsConnectionConfig[F]().map(new KGDatasetInfoFinderImpl(_))
}

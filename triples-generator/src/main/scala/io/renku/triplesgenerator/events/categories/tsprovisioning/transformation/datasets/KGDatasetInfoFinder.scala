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

package io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.datasets

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.renku.graph.model.datasets.{DateCreated, Description, InternalSameAs, OriginalIdentifier, ResourceId, SameAs, TopmostSameAs}
import io.renku.graph.model.persons
import io.renku.graph.model.views.RdfResource
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGDatasetInfoFinder[F[_]] {
  def findParentTopmostSameAs(idSameAs: InternalSameAs)(implicit ev: InternalSameAs.type): F[Option[TopmostSameAs]]
  def findTopmostSameAs(resourceId:     ResourceId)(implicit ev:     ResourceId.type):     F[Set[TopmostSameAs]]
  def findDatasetCreators(resourceId:   ResourceId): F[Set[persons.ResourceId]]
  def findDatasetOriginalIdentifiers(resourceId: ResourceId): F[Set[OriginalIdentifier]]
  def findDatasetDateCreated(resourceId:         ResourceId): F[Set[DateCreated]]
  def findDatasetDescriptions(resourceId:        ResourceId): F[Set[Description]]
  def findDatasetSameAs(resourceId:              ResourceId): F[Set[SameAs]]
}

private class KGDatasetInfoFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    rdfStoreConfig: RdfStoreConfig
) extends RdfStoreClientImpl(rdfStoreConfig)
    with KGDatasetInfoFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas.{renku, schema}
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def findParentTopmostSameAs(sameAs: InternalSameAs)(implicit
      ev:                                      InternalSameAs.type
  ): F[Option[TopmostSameAs]] = {
    implicit val decoder: Decoder[Option[TopmostSameAs]] = ResultsDecoder[Option, TopmostSameAs] { implicit cur =>
      extract[TopmostSameAs]("topmostSameAs")
    }(toOption(onMultiple = show"More than one topmostSameAs found for dataset ${sameAs.show}"))

    queryExpecting[Option[TopmostSameAs]](using = queryFindingSameAs(sameAs.value))
  }

  override def findTopmostSameAs(resourceId: ResourceId)(implicit
      ev:                                    ResourceId.type
  ): F[Set[TopmostSameAs]] = {
    implicit val decoder: Decoder[Set[TopmostSameAs]] = ResultsDecoder[Set, TopmostSameAs] { implicit cur =>
      extract[TopmostSameAs]("topmostSameAs")
    }

    queryExpecting[Set[TopmostSameAs]](using = queryFindingSameAs(resourceId.value))
  }

  private def queryFindingSameAs(resourceId: String) = SparqlQuery.of(
    name = "transformation - find topmostSameAs",
    Prefixes.of(renku -> "renku", schema -> "schema"),
    s"""|SELECT ?topmostSameAs
        |WHERE {
        |  <$resourceId> a schema:Dataset;
        |                renku:topmostSameAs ?topmostSameAs.
        |}
        |""".stripMargin
  )

  override def findDatasetCreators(resourceId: ResourceId): F[Set[persons.ResourceId]] = {
    implicit val creatorsDecoder: Decoder[Set[persons.ResourceId]] = ResultsDecoder[Set, persons.ResourceId] {
      implicit cur => extract[persons.ResourceId]("personId")
    }

    queryExpecting[Set[persons.ResourceId]] {
      SparqlQuery.of(
        name = "transformation - find ds creators",
        Prefixes of schema -> "schema",
        s"""|SELECT ?personId
            |WHERE {
            |  ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                    schema:creator ?personId
            |}
            |""".stripMargin
      )
    }
  }

  override def findDatasetOriginalIdentifiers(resourceId: ResourceId): F[Set[OriginalIdentifier]] = {
    implicit val decoder: Decoder[Set[OriginalIdentifier]] = ResultsDecoder[Set, OriginalIdentifier] {
      implicit cursor => extract("originalId")
    }
    queryExpecting[Set[OriginalIdentifier]] {
      SparqlQuery.of(
        name = "transformation - find ds originalIdentifiers",
        Prefixes of (renku -> "renku", schema -> "schema"),
        s"""|SELECT ?originalId
            |WHERE {
            |  ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                    renku:originalIdentifier ?originalId.
            |}
            |""".stripMargin
      )
    }
  }

  def findDatasetDateCreated(resourceId: ResourceId): F[Set[DateCreated]] = {
    implicit val decoder: Decoder[Set[DateCreated]] = ResultsDecoder[Set, DateCreated] { implicit cursor =>
      extract("date")
    }
    queryExpecting[Set[DateCreated]] {
      SparqlQuery.of(
        name = "transformation - find ds originalIdentifiers",
        Prefixes of schema -> "schema",
        s"""|SELECT ?date
            |WHERE {
            |  ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                    schema:dateCreated ?date.
            |}
            |""".stripMargin
      )
    }
  }

  override def findDatasetDescriptions(resourceId: ResourceId): F[Set[Description]] = {
    implicit val decoder: Decoder[Set[Description]] = ResultsDecoder[Set, Description] { implicit cursor =>
      extract("desc")
    }
    queryExpecting[Set[Description]] {
      SparqlQuery.of(
        name = "transformation - find ds descriptions",
        Prefixes of schema -> "schema",
        s"""|SELECT ?desc
            |WHERE {
            |  ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                    schema:description ?desc.
            |}
            |""".stripMargin
      )
    }
  }

  override def findDatasetSameAs(resourceId: ResourceId): F[Set[SameAs]] = {
    implicit val decoder: Decoder[Set[SameAs]] = ResultsDecoder[Set, SameAs] { implicit cursor =>
      extract("sameAs")
    }
    queryExpecting[Set[SameAs]] {
      SparqlQuery.of(
        name = "transformation - find ds sameAs",
        Prefixes of schema -> "schema",
        s"""|SELECT ?sameAs
            |WHERE {
            |  ${resourceId.showAs[RdfResource]} a schema:Dataset;
            |                                    schema:sameAs ?sameAs.
            |}
            |""".stripMargin
      )
    }
  }
}

private object KGDatasetInfoFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGDatasetInfoFinder[F]] = for {
    config <- RdfStoreConfig[F]()
  } yield new KGDatasetInfoFinderImpl(config)
}

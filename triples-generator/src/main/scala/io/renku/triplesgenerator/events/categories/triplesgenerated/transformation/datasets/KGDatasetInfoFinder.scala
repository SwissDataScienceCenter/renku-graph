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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.datasets

import cats.effect.Async
import cats.syntax.all._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.graph.model.datasets.{InternalSameAs, ResourceId, SameAs, TopmostSameAs}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGDatasetInfoFinder[F[_]] {
  def findParentTopmostSameAs(idSameAs: InternalSameAs)(implicit ev: InternalSameAs.type): F[Option[TopmostSameAs]]
  def findTopmostSameAs(resourceId:     ResourceId)(implicit ev:     ResourceId.type):     F[Option[TopmostSameAs]]
}

private class KGDatasetInfoFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with KGDatasetInfoFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.renku.graph.model.Schemas.{renku, schema}
  import io.renku.tinytypes.json.TinyTypeDecoders._

  override def findParentTopmostSameAs(sameAs: InternalSameAs)(implicit
      ev:                                      InternalSameAs.type
  ): F[Option[TopmostSameAs]] =
    queryExpecting[Set[TopmostSameAs]](using = queryFindingSameAs(sameAs.value))
      .flatMap(toOption[TopmostSameAs, InternalSameAs](sameAs))

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

  override def findTopmostSameAs(resourceId: ResourceId)(implicit
      ev:                                    ResourceId.type
  ): F[Option[TopmostSameAs]] =
    queryExpecting[Set[TopmostSameAs]](using = queryFindingSameAs(resourceId.value))
      .flatMap(toOption[TopmostSameAs, ResourceId](resourceId))

  private implicit val topmostSameAsDecoder: Decoder[Set[TopmostSameAs]] = {
    val topmostSameAs: Decoder[Option[TopmostSameAs]] =
      _.downField("topmostSameAs").downField("value").as[Option[TopmostSameAs]]
    _.downField("results").downField("bindings").as(decodeList(topmostSameAs)).map(_.flatten.toSet)
  }

  private def toOption[T, ID](id: ID)(implicit entityTypeInfo: ID => String): Set[T] => F[Option[T]] = {
    case set if set.isEmpty   => Option.empty[T].pure[F]
    case set if set.size == 1 => set.headOption.pure[F]
    case _ =>
      new Exception(
        s"More than one ${entityTypeInfo(id)} found for dataset $id"
      ).raiseError[F, Option[T]]
  }

  private implicit val topmostSameAsInfo: SameAs => String     = _ => "topmostSameAs"
  private implicit val resourceIdInfo:    ResourceId => String = _ => "resourceId"
}

private object KGDatasetInfoFinder {
  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[KGDatasetInfoFinder[F]] = for {
    config <- RdfStoreConfig[F]()
  } yield new KGDatasetInfoFinderImpl(config, timeRecorder)
}

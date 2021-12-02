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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.activities

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import io.renku.graph.model.Schemas.{prov, schema}
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{activities, users}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGInfoFinder[F[_]] {
  def findActivityAuthor(resourceId: activities.ResourceId): F[Option[users.ResourceId]]
}

private object KGInfoFinder {
  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[KGInfoFinder[F]] = for {
    config <- RdfStoreConfig[F]()
  } yield new KGInfoFinderImpl(config, timeRecorder)
}

private class KGInfoFinderImpl[F[_]: Async: Logger](rdfStoreConfig: RdfStoreConfig,
                                                    timeRecorder: SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with KGInfoFinder[F] {

  def findActivityAuthor(resourceId: activities.ResourceId): F[Option[users.ResourceId]] =
    queryExpecting[Set[users.ResourceId]](using = queryFindingAuthor(resourceId))
      .flatMap(toOption[users.ResourceId, activities.ResourceId](resourceId))

  private def queryFindingAuthor(resourceId: activities.ResourceId) = SparqlQuery.of(
    name = "transformation - find activity author",
    Prefixes of (schema -> "schema", prov -> "prov"),
    s"""|SELECT ?personId
        |WHERE {
        |  ${resourceId.showAs[RdfResource]} a prov:Activity;
        |                                    prov:wasAssociatedWith ?personId.
        |  ?personId a schema:Person.
        |}
        |""".stripMargin
  )

  private implicit val authorsDecoder: Decoder[Set[users.ResourceId]] = {
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val authorId: Decoder[users.ResourceId] =
      _.downField("personId").downField("value").as[users.ResourceId]

    _.downField("results").downField("bindings").as(decodeList(authorId)).map(_.toSet)
  }

  private def toOption[T, ID](id: ID)(implicit entityTypeInfo: Set[T] => String): Set[T] => F[Option[T]] = {
    case set if set.isEmpty   => Option.empty[T].pure[F]
    case set if set.size == 1 => set.headOption.pure[F]
    case set => new Exception(s"More than one ${entityTypeInfo(set)} found for activity $id").raiseError[F, Option[T]]
  }

  private implicit val usersResourceIdInfo: Set[users.ResourceId] => String = _ => "author ResourceId"
}

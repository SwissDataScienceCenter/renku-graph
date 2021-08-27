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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.datasets

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.model.datasets.{DerivedFrom, InternalSameAs, ResourceId, SameAs, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import io.circe.Decoder
import io.circe.Decoder.decodeList
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait KGDatasetInfoFinder[Interpretation[_]] {
  def findParentTopmostSameAs(idSameAs: InternalSameAs)(implicit
      ev:                               InternalSameAs.type
  ): Interpretation[Option[TopmostSameAs]]
  def findTopmostSameAs(resourceId: ResourceId)(implicit
      ev:                           ResourceId.type
  ): Interpretation[Option[TopmostSameAs]]
  def findParentTopmostDerivedFrom(derivedFrom: DerivedFrom)(implicit
      ev:                                       DerivedFrom.type
  ): Interpretation[Option[TopmostDerivedFrom]]
  def findTopmostDerivedFrom(resourceId: ResourceId)(implicit
      ev:                                ResourceId.type
  ): Interpretation[Option[TopmostDerivedFrom]]
}

private class KGDatasetInfoFinderImpl(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder)
    with KGDatasetInfoFinder[IO] {

  import cats.syntax.all._
  import ch.datascience.graph.model.Schemas.{renku, schema}
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import eu.timepit.refined.auto._

  override def findParentTopmostSameAs(sameAs: InternalSameAs)(implicit
      ev:                                      InternalSameAs.type
  ): IO[Option[TopmostSameAs]] =
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
  ): IO[Option[TopmostSameAs]] =
    queryExpecting[Set[TopmostSameAs]](using = queryFindingSameAs(resourceId.value))
      .flatMap(toOption[TopmostSameAs, ResourceId](resourceId))

  private implicit val topmostSameAsDecoder: Decoder[Set[TopmostSameAs]] = {
    val topmostSameAs: Decoder[Option[TopmostSameAs]] =
      _.downField("topmostSameAs").downField("value").as[Option[TopmostSameAs]]
    _.downField("results").downField("bindings").as(decodeList(topmostSameAs)).map(_.flatten.toSet)
  }

  override def findParentTopmostDerivedFrom(derivedFrom: DerivedFrom)(implicit
      ev:                                                DerivedFrom.type
  ): IO[Option[TopmostDerivedFrom]] =
    queryExpecting[Set[TopmostDerivedFrom]](using = queryFindingDerivedFrom(derivedFrom.value))
      .flatMap(toOption[TopmostDerivedFrom, DerivedFrom](derivedFrom))

  private def queryFindingDerivedFrom(resourceIdAsString: String) = SparqlQuery.of(
    name = "transformation - ds topmostDerivedFrom",
    Prefixes.of(renku -> "renku", schema -> "schema"),
    s"""|SELECT ?topmostDerivedFrom
        |WHERE {
        |  <$resourceIdAsString> a schema:Dataset;
        |                        renku:topmostDerivedFrom ?topmostDerivedFrom.
        |}
        |""".stripMargin
  )

  def findTopmostDerivedFrom(resourceId: ResourceId)(implicit
      ev:                                ResourceId.type
  ): IO[Option[TopmostDerivedFrom]] =
    queryExpecting[Set[TopmostDerivedFrom]](using = queryFindingDerivedFrom(resourceId.value))
      .flatMap(toOption[TopmostDerivedFrom, ResourceId](resourceId))

  private implicit val topmostDerivedDecoder: Decoder[Set[TopmostDerivedFrom]] = {
    val topmostDerivedFrom: Decoder[Option[TopmostDerivedFrom]] =
      _.downField("topmostDerivedFrom").downField("value").as[Option[TopmostDerivedFrom]]
    _.downField("results").downField("bindings").as(decodeList(topmostDerivedFrom)).map(_.flatten.toSet)
  }

  private def toOption[T, ID](id: ID)(implicit entityTypeInfo: ID => String): Set[T] => IO[Option[T]] = {
    case set if set.isEmpty   => Option.empty[T].pure[IO]
    case set if set.size == 1 => set.headOption.pure[IO]
    case _ =>
      new Exception(
        s"More than one ${entityTypeInfo(id)} found for dataset $id"
      ).raiseError[IO, Option[T]]
  }

  private implicit val topmostSameAsInfo:      SameAs => String      = _ => "topmostSameAs"
  private implicit val resourceIdInfo:         ResourceId => String  = _ => "resourceId"
  private implicit val topmostDerivedFromInfo: DerivedFrom => String = _ => "topmostDerivedFrom"
}

private object IOKGDatasetInfoFinder {
  def apply(logger:     Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[KGDatasetInfoFinderImpl] =
    RdfStoreConfig[IO]() map (new KGDatasetInfoFinderImpl(_, logger, timeRecorder))
}

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
import ch.datascience.graph.model.datasets.{DerivedFrom, InternalSameAs, SameAs, TopmostDerivedFrom, TopmostSameAs}
import ch.datascience.rdfstore._
import org.typelevel.log4cats.Logger
import io.circe.Decoder
import io.circe.Decoder.decodeList

import scala.concurrent.ExecutionContext

private trait KGDatasetInfoFinder[Interpretation[_]] {
  def findTopmostSameAs(idSameAs: InternalSameAs): Interpretation[Option[TopmostSameAs]]

  def findTopmostDerivedFrom(derivedFrom: DerivedFrom): Interpretation[Option[TopmostDerivedFrom]]
}

private class KGDatasetInfoFinderImpl(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with KGDatasetInfoFinder[IO] {

  import cats.syntax.all._
  import ch.datascience.tinytypes.json.TinyTypeDecoders._
  import eu.timepit.refined.auto._

  override def findTopmostSameAs(sameAs: InternalSameAs): IO[Option[TopmostSameAs]] =
    queryExpecting[Set[TopmostSameAs]](using = queryFindingSameAs(sameAs))
      .flatMap(toOption[TopmostSameAs, InternalSameAs](sameAs))

  private def queryFindingSameAs(sameAs: InternalSameAs) = SparqlQuery(
    name = "upload - ds topmostSameAs",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>",
      "PREFIX schema: <http://schema.org/>"
    ),
    s"""|SELECT ?maybeTopmostSameAs
        |WHERE {
        |  <$sameAs> rdf:type <http://schema.org/Dataset>;
        |            renku:topmostSameAs ?maybeTopmostSameAs.
        |}
        |""".stripMargin
  )

  private implicit val topmostSameAsDecoder: Decoder[Set[TopmostSameAs]] = {
    val topmostSameAs: Decoder[Option[TopmostSameAs]] =
      _.downField("maybeTopmostSameAs").downField("value").as[Option[TopmostSameAs]]
    _.downField("results").downField("bindings").as(decodeList(topmostSameAs)).map(_.flatten.toSet)
  }

  override def findTopmostDerivedFrom(derivedFrom: DerivedFrom): IO[Option[TopmostDerivedFrom]] =
    queryExpecting[Set[TopmostDerivedFrom]](using = queryFindingDerivedFrom(derivedFrom))
      .flatMap(toOption[TopmostDerivedFrom, DerivedFrom](derivedFrom))

  private def queryFindingDerivedFrom(derivedFrom: DerivedFrom) = SparqlQuery(
    name = "upload - ds topmostDerivedFrom",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
    ),
    s"""|SELECT ?maybeTopmostDerivedFrom
        |WHERE {
        |  <$derivedFrom> rdf:type <http://schema.org/Dataset>;
        |                 renku:topmostDerivedFrom ?maybeTopmostDerivedFrom.
        |}
        |""".stripMargin
  )

  private implicit val topmostDerivedDecoder: Decoder[Set[TopmostDerivedFrom]] = {
    val topmostDerivedFrom: Decoder[Option[TopmostDerivedFrom]] =
      _.downField("maybeTopmostDerivedFrom").downField("value").as[Option[TopmostDerivedFrom]]
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
  private implicit val topmostDerivedFromInfo: DerivedFrom => String = _ => "topmostDerivedFrom"
}

private object IOKGDatasetInfoFinder {
  def apply(logger:     Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[KGDatasetInfoFinderImpl] =
    for {
      rdfStoreConfig <- RdfStoreConfig[IO]()
    } yield new KGDatasetInfoFinderImpl(rdfStoreConfig, logger, timeRecorder)
}

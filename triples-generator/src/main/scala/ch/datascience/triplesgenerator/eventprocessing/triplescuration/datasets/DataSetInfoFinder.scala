/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package datasets

import cats.MonadError
import cats.data.OptionT
import cats.implicits._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.DataSetInfoFinder.DatasetInfo
import io.circe.optics.JsonOptics._
import io.circe.{Decoder, Json}
import io.renku.jsonld.EntityId
import monocle.function.Plated

import scala.collection.mutable
import scala.language.higherKinds

private trait DataSetInfoFinder[Interpretation[_]] {
  def findDatasetsInfo(triples: JsonLDTriples): Interpretation[Set[DatasetInfo]]
}

private class DataSetInfoFinderImpl[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable])
    extends DataSetInfoFinder[Interpretation] {

  import CollectedInfo._

  def findDatasetsInfo(triples: JsonLDTriples): Interpretation[Set[DatasetInfo]] =
    triples
      .fold(mutable.HashSet.empty[CollectedInfo])(collectDatasetInfo)
      .map(convertToDatasetInfos)

  private def collectDatasetInfo(collected: mutable.Set[CollectedInfo]): Json => Interpretation[Json] = { json =>
    json.findTypes match {
      case types if types contains (schema / "Dataset").toString =>
        json
          .getId[Interpretation, EntityId]
          .semiflatMap { collectDatasetInfo(collected, json) }
          .fold(json)(_ => json)
      case types if types contains (schema / "URL").toString =>
        json
          .getId[Interpretation, EntityId]
          .semiflatMap { collectSameAsInfo(collected, json) }
          .fold(json)(_ => json)
      case _ => json.pure[Interpretation]
    }
  }

  private def collectDatasetInfo(
      collected: mutable.Set[CollectedInfo],
      json:      Json
  )(entityId:    EntityId): Interpretation[Unit] =
    for {
      maybeSameAsUrl   <- getSameAsUrl(json).value
      maybeDerivedFrom <- getDerivedFrom(json).value
      _ = collected add CollectedDatasetInfo(entityId, maybeSameAsUrl, maybeDerivedFrom)
    } yield ()

  private def getSameAsUrl(json: Json): OptionT[Interpretation, EntityId] =
    json.get[Json]((schema / "sameAs").toString) match {
      case Some(sameAs) => sameAs.getId[Interpretation, EntityId]
      case None         => OptionT.none[Interpretation, EntityId]
    }

  private def getDerivedFrom(json: Json): OptionT[Interpretation, DerivedFrom] =
    json.get[Json]((prov / "wasDerivedFrom").toString) match {
      case Some(derivedFrom) => derivedFrom.getId[Interpretation, DerivedFrom]
      case None              => OptionT.none[Interpretation, DerivedFrom]
    }

  private def collectSameAsInfo(
      collected: mutable.Set[CollectedInfo],
      json:      Json
  )(entityId:    EntityId): Interpretation[Unit] =
    getSameAs(json)
      .map {
        case Some(sameAs) => collected add CollectedSameAs(entityId, sameAs)
        case None         => ()
      }
      .map(_ => ())

  private def getSameAs(urlJson: Json): Interpretation[Option[SameAs]] = {
    val idSameAs: Decoder[SameAs] = Decoder.decodeString.emap(v => SameAs.fromId(v).leftMap(_.toString))

    urlJson.get[Json]((schema / "url").toString) match {
      case None       => OptionT.none[Interpretation, SameAs]
      case Some(json) => json.getValue[Interpretation, SameAs] orElse json.getId[Interpretation, SameAs](idSameAs, ME)
    }
  }.value

  private implicit class JsonLDTriplesOps(triples: JsonLDTriples) {
    def fold(
        zero: mutable.Set[CollectedInfo]
    )(f:      mutable.Set[CollectedInfo] => Json => Interpretation[Json]): Interpretation[Set[CollectedInfo]] =
      Plated.transformM[Json, Interpretation](f(zero))(triples.value).map(_ => zero.toSet)
  }

  private def convertToDatasetInfos(collectedInfos: Set[CollectedInfo]) =
    collectedInfos.foldLeft(Set.empty[DatasetInfo]) {
      case (finalInfos, CollectedDatasetInfo(dsId, maybeSameAsUrl, maybeDerivedFrom)) =>
        finalInfos + ((dsId, maybeSameAsUrl.findMatchingSameAs(collectedInfos), maybeDerivedFrom))
      case (finalInfos, _) => finalInfos
    }

  private implicit class SameAsUrlOps(maybeSameAsUrl: Option[EntityId]) {

    def findMatchingSameAs(collectedInfos: Set[CollectedInfo]): Option[SameAs] =
      for {
        sameAsUrl <- maybeSameAsUrl
        matchingSameAs <- collectedInfos.flatMap {
                           case CollectedSameAs(`sameAsUrl`, sameAs) => sameAs.some
                           case _                                    => None
                         }.headOption
      } yield matchingSameAs
  }

  private trait CollectedInfo

  private object CollectedInfo {

    case class CollectedDatasetInfo(
        id:               EntityId,
        maybeSameAsUrl:   Option[EntityId],
        maybeDerivedFrom: Option[DerivedFrom]
    ) extends CollectedInfo

    case class CollectedSameAs(sameAsUrl: EntityId, sameAs: SameAs) extends CollectedInfo
  }
}

private object DataSetInfoFinder {
  type DatasetInfo = (EntityId, Option[SameAs], Option[DerivedFrom])
}

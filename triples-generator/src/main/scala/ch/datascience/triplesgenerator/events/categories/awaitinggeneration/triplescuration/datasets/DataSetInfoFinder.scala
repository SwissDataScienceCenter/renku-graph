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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration
package datasets

import cats.MonadError
import cats.data.OptionT
import cats.syntax.all._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.datasets.{DerivedFrom, SameAs}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.tinytypes.TinyType
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.datasets.DataSetInfoFinder.DatasetInfo
import io.circe.optics.JsonOptics._
import io.circe.{Decoder, Json}
import io.renku.jsonld.EntityId
import monocle.function.Plated

import scala.collection.mutable

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
          .semiflatMap(collectDatasetInfo(collected, json))
          .fold(json)(_ => json)
      case types if types contains (schema / "URL").toString =>
        json
          .getId[Interpretation, EntityId]
          .semiflatMap(collectEntityInfo[SameAs](collected, json, SameAs.fromId))
          .semiflatMap(collectEntityInfo[DerivedFrom](collected, json, DerivedFrom.from))
          .fold(json)(_ => json)
      case _ => json.pure[Interpretation]
    }
  }

  private def collectDatasetInfo(
      collected: mutable.Set[CollectedInfo],
      json:      Json
  )(entityId:    EntityId): Interpretation[Unit] =
    for {
      maybeSameAsUrl      <- getSameAsUrl(json).value
      maybeDerivedFromUrl <- getDerivedFromUrl(json).value
      _ = collected add CollectedDatasetInfo(entityId, maybeSameAsUrl, maybeDerivedFromUrl)
    } yield ()

  private def getSameAsUrl(json: Json): OptionT[Interpretation, EntityId] =
    json.get[Json]((schema / "sameAs").toString) match {
      case Some(sameAs) => sameAs.getId[Interpretation, EntityId]
      case None         => OptionT.none[Interpretation, EntityId]
    }

  private def getDerivedFromUrl(json: Json): OptionT[Interpretation, EntityId] =
    json.get[Json]((prov / "wasDerivedFrom").toString) match {
      case Some(derivedFrom) => derivedFrom.getId[Interpretation, EntityId]
      case None              => OptionT.none[Interpretation, EntityId]
    }

  private def collectEntityInfo[T](
      collected: mutable.Set[CollectedInfo],
      json:      Json,
      fromId:    String => Either[IllegalArgumentException, T]
  )(entityId:    EntityId)(implicit decoder: Decoder[T]): Interpretation[EntityId] =
    getEntity[T](json, fromId)
      .map {
        case Some(entity) => collected add CollectedEntity(entityId, entity)
        case None         => ()
      }
      .map(_ => entityId)

  private def getEntity[T](
      urlJson:        Json,
      fromId:         String => Either[IllegalArgumentException, T]
  )(implicit decoder: Decoder[T]): Interpretation[Option[T]] = {
    val idSameAs: Decoder[T] = Decoder.decodeString.emap(value => fromId(value).leftMap(_.toString))

    urlJson.get[Json]((schema / "url").toString) match {
      case None       => OptionT.none[Interpretation, T]
      case Some(json) => json.getValue[Interpretation, T] orElse json.getId[Interpretation, T](idSameAs, ME)
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
      case (finalInfos, CollectedDatasetInfo(dsId, maybeSameAsUrl, maybeDerivedFromUrl)) =>
        finalInfos + ((dsId,
                       maybeSameAsUrl.findMatchingEntity[SameAs](collectedInfos),
                       maybeDerivedFromUrl.findMatchingEntity[DerivedFrom](collectedInfos)
                      )
        )
      case (finalInfos, _) => finalInfos
    }

  private implicit class UrlOps(maybeEntityId: Option[EntityId]) {

    def findMatchingEntity[T <: TinyType](
        collectedInfos:                   Set[CollectedInfo]
    )(implicit collectedEntityIdToEntity: EntityId => CollectedInfo => Option[T]): Option[T] =
      for {
        entityId       <- maybeEntityId
        matchingSameAs <- collectedInfos.flatMap(collectedEntityIdToEntity(entityId)).headOption

      } yield matchingSameAs
  }

  private implicit val collectedSameAsIdToEntity: EntityId => CollectedInfo => Option[SameAs] =
    entityId => {
      case CollectedEntity(`entityId`, entity: SameAs) => entity.some
      case _ => None
    }

  private implicit val collectedDerivedFromIdToEntity: EntityId => CollectedInfo => Option[DerivedFrom] =
    entityId => {
      case CollectedEntity(`entityId`, entity: DerivedFrom) => entity.some
      case _ => None
    }

  private trait CollectedInfo

  private object CollectedInfo {

    case class CollectedDatasetInfo(
        id:                  EntityId,
        maybeSameAsUrl:      Option[EntityId],
        maybeDerivedFromUrl: Option[EntityId]
    ) extends CollectedInfo

    case class CollectedEntity[T](entityId: EntityId, entity: T) extends CollectedInfo
  }
}

private object DataSetInfoFinder {
  type DatasetInfo = (EntityId, Option[SameAs], Option[DerivedFrom])
}

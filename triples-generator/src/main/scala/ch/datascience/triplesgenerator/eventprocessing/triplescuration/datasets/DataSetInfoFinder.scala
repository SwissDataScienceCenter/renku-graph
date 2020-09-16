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
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.DataSetInfoFinder.DatasetInfo
import io.circe.Decoder.decodeString
import io.circe.Json
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.renku.jsonld.EntityId
import monocle.function.Plated

import scala.collection.mutable
import scala.language.higherKinds

private trait DataSetInfoFinder[Interpretation[_]] {
  def findDatasetsInfo(triples: JsonLDTriples): Interpretation[Set[DatasetInfo]]
}
private class DataSetInfoFinderImpl[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable])
    extends DataSetInfoFinder[Interpretation] {

  def findDatasetsInfo(triples: JsonLDTriples): Interpretation[Set[DatasetInfo]] =
    triples.fold(mutable.HashSet.empty[DatasetInfo])(collectDatasetInfo)

  private def collectDatasetInfo(collected: mutable.Set[DatasetInfo]): Json => Interpretation[Json] = { json =>
    root.`@type`.each.string.getAll(json) match {
      case types if types.contains((schema / "Dataset").toString) =>
        json.get[EntityId]("@id") match {
          case Some(entityId) =>
            for {
              maybeSameAs      <- getSameAs(json)
              maybeDerivedFrom <- getDerivedFrom(json)
              _ = collected.add((entityId, maybeSameAs, maybeDerivedFrom))
            } yield json
          case None => json.pure[Interpretation]
        }
      case _ => json.pure[Interpretation]
    }
  }

  private def getSameAs(json: Json): Interpretation[Option[SameAs]] =
    json.get[Json]((schema / "sameAs").toString) match {
      case Some(sameAs) =>
        sameAs
          .getValue[Interpretation, SameAs]((schema / "url").toString)
          .orElse(getIdSameAs(sameAs))
          .value
      case None => Option.empty[SameAs].pure[Interpretation]
    }

  private def getIdSameAs(json: Json): OptionT[Interpretation, SameAs] =
    json.get[Json]((schema / "url").toString) match {
      case Some(value) =>
        value.get[String]("@id") match {
          case Some(id) =>
            SameAs
              .from(id)
              .fold(e => OptionT.liftF(e.raiseError[Interpretation, SameAs]), OptionT.some[Interpretation](_))
          case None => OptionT.none[Interpretation, SameAs]
        }
      case None => OptionT.none[Interpretation, SameAs]
    }

  private def getDerivedFrom(json: Json): Interpretation[Option[DerivedFrom]] =
    json.get[Json]((prov / "wasDerivedFrom").toString) match {
      case Some(derivedFrom) => derivedFrom.get[DerivedFrom]("@id").pure[Interpretation]
      case None              => Option.empty[DerivedFrom].pure[Interpretation]
    }

  private implicit class JsonLDTriplesOps(triples: JsonLDTriples) {
    def fold(
        zero: mutable.Set[DatasetInfo]
    )(f:      mutable.Set[DatasetInfo] => Json => Interpretation[Json]): Interpretation[Set[DatasetInfo]] =
      Plated.transformM[Json, Interpretation](f(zero))(triples.value).map(_ => zero.toSet)
  }
}

private object DataSetInfoFinder {
  type DatasetInfo = (EntityId, Option[SameAs], Option[DerivedFrom])
}

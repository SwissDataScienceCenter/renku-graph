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

package io.renku.entities.searchgraphs
package commands

import Encoders._
import SearchInfoLens._
import UpdateCommand._
import cats.MonadThrow
import cats.syntax.all._
import io.renku.entities.searchgraphs.SearchInfoOntology.linkProperty
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.{Quad, TripleObject}
import io.renku.triplesstore.client.syntax._

private object CommandsCalculator {

  def calculateCommands[F[_]: MonadThrow]: CalculatorInfoSet => F[List[UpdateCommand]] = {
    case `DS present on Project & TS`()                      => List.empty[UpdateCommand].pure[F]
    case `DS present on Project only`(info)                  => info.asQuads.map(Insert).toList.pure[F].widen
    case `DS present on TS only on the single project`(info) => info.asQuads.map(Delete).toList.pure[F].widen
    case `DS present on TS only on many projects`(info, link) =>
      (link.asQuads + quad(info, linkProperty, link.resourceId.asEntityId)).map(Delete).toList.pure[F].widen
    case infoSet =>
      new IllegalStateException(show"Cannot calculate update commands for $infoSet").raiseError[F, List[UpdateCommand]]
  }

  private object `DS present on Project & TS` {
    def unapply(infoSet: CalculatorInfoSet): Boolean = infoSet match {
      case CalculatorInfoSet(_, Some(modelInfo), Some(tsInfo)) =>
        (findLink(infoSet.project.resourceId)(modelInfo) -> findLink(infoSet.project.resourceId)(tsInfo))
          .mapN((_, _) => true)
          .getOrElse(false)
      case _ => false
    }
  }
  private object `DS present on Project only` {
    def unapply(infoSet: CalculatorInfoSet): Option[SearchInfo] = infoSet match {
      case CalculatorInfoSet(_, someModelInfo @ Some(_), None) => someModelInfo
      case _                                                   => None
    }
  }
  private object `DS present on TS only on the single project` {
    def unapply(infoSet: CalculatorInfoSet): Option[SearchInfo] = infoSet match {
      case CalculatorInfoSet(_, None, Some(tsInfo)) if tsInfo.links.size == 1 =>
        findLink(infoSet.project.resourceId)(tsInfo).map(_ => tsInfo)
      case _ => None
    }
  }
  private object `DS present on TS only on many projects` {
    def unapply(infoSet: CalculatorInfoSet): Option[(SearchInfo, Link)] = infoSet match {
      case CalculatorInfoSet(_, None, Some(tsInfo)) =>
        findLink(infoSet.project.resourceId)(tsInfo).map(tsInfo -> _)
      case _ => None
    }
  }

  private def quad(info: SearchInfo, property: Property, obj: TripleObject): Quad =
    DatasetsQuad(info.topmostSameAs, property, obj)
}

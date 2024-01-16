/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.entities.searchgraphs.datasets.commands

import Encoders._
import cats.syntax.all._
import io.renku.entities.searchgraphs.UpdateCommand
import io.renku.entities.searchgraphs.UpdateCommand._
import io.renku.entities.searchgraphs.datasets.{DatasetSearchInfo, Link, links}
import io.renku.graph.model.datasets
import io.renku.triplesstore.client.syntax._

private trait CommandsCalculator {
  def calculateCommands: CalculatorInfoSet => List[UpdateCommand]
}

private object CommandsCalculator extends CommandsCalculator {

  def calculateCommands: CalculatorInfoSet => List[UpdateCommand] = {
    case `DS exists on Project only`(info) =>
      info.asQuads.map(Insert).toList.widen
    case `DS exists only on the project in TS`(topSameAs) =>
      List(deleteInfo(topSameAs)).widen
    case `DS exists only on other projects in TS`((topSameAs, linkId, links)) =>
      deleteLink(topSameAs, linkId) :::
        deleteProjectsVisibilitiesConcatQuad(topSameAs) :::
        insertProjectsVisibilitiesConcatQuad(topSameAs, links)
    case infoSet =>
      deleteInfo(infoSet.topmostSameAs) ::
        infoSet.asDatasetSearchInfo.fold(List.empty[UpdateCommand])(_.asQuads.map(Insert).toList)
  }

  private object `DS exists on Project only` {
    def unapply(infoSet: CalculatorInfoSet): Option[DatasetSearchInfo] = infoSet match {
      case CalculatorInfoSet.ModelInfoOnly(_, _) => infoSet.asDatasetSearchInfo
      case _                                     => None
    }
  }

  private object `DS exists only on the project in TS` {
    def unapply(infoSet: CalculatorInfoSet): Option[datasets.TopmostSameAs] = infoSet match {
      case CalculatorInfoSet.TSInfoOnly(_, tsInfo)
          if tsInfo.links.map(_.projectId) == List(infoSet.project.resourceId) =>
        tsInfo.topmostSameAs.some
      case _ => None
    }
  }

  private object `DS exists only on other projects in TS` {
    def unapply(infoSet: CalculatorInfoSet): Option[(datasets.TopmostSameAs, links.ResourceId, List[Link])] =
      infoSet match {
        case CalculatorInfoSet.TSInfoOnly(projectId, tsInfo) =>
          tsInfo.links
            .find(_.projectId == projectId.resourceId)
            .map(link => (infoSet.topmostSameAs, link.resourceId, tsInfo.links))
        case _ => None
      }
  }

  private def deleteInfo(topSameAs: datasets.TopmostSameAs) =
    UpdateCommand.Query(DatasetInfoDeleteQuery(topSameAs))

  private def deleteLink(topSameAs: datasets.TopmostSameAs, linkId: links.ResourceId) =
    List(UpdateCommand.Query(DatasetLinkDeleteQuery(topSameAs, linkId)))

  private def deleteProjectsVisibilitiesConcatQuad(topSameAs: datasets.TopmostSameAs) =
    List(UpdateCommand.Query(SlugVisibilitiesConcatDeleteQuery(topSameAs)))

  private def insertProjectsVisibilitiesConcatQuad(topSameAs: datasets.TopmostSameAs, links: List[Link]) =
    (topSameAs -> links).asQuads.map(Insert).toList
}

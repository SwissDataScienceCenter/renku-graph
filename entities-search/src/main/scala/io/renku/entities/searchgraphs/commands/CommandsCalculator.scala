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
import SearchInfoOntology.{linkProperty, visibilityProperty}
import UpdateCommand._
import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.{datasets, projects}
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.{Quad, TripleObject}
import io.renku.triplesstore.client.syntax._

private trait CommandsCalculator[F[_]] {
  def calculateCommands: CalculatorInfoSet => F[List[UpdateCommand]]
}

private object CommandsCalculator {
  def apply[F[_]: MonadThrow](): CommandsCalculator[F] = new CommandsCalculatorImpl[F]
}

private class CommandsCalculatorImpl[F[_]: MonadThrow] extends CommandsCalculator[F] {

  def calculateCommands: CalculatorInfoSet => F[List[UpdateCommand]] = {
    case `DS exists on Project only`(info)                     => info.asQuads.map(Insert).toList.pure[F].widen
    case `DS exists on Project & TS - no visibility changes`() => noUpdates
    case `DS exists on Project & TS but on other narrower visibility Projects`(topSameAs,
                                                                               oldVisibility,
                                                                               newVisibility,
                                                                               newLink
        ) =>
      (deleteInfoVisibility(topSameAs, oldVisibility) ::
        insertInfoVisibility(topSameAs, newVisibility) ::
        insertLink(topSameAs, newLink)).pure[F]
    case `DS exists on Project & TS but on other broader or same visibility Projects`(topSameAs, newLink) =>
      insertLink(topSameAs, newLink).pure[F].widen
    case `DS exists on Project & TS - visibility change affects TS`(topSameAs, oldVisibility, newVisibility) =>
      (deleteInfoVisibility(topSameAs, oldVisibility) ::
        insertInfoVisibility(topSameAs, newVisibility) :: Nil).pure[F]
    case `DS exists on TS only on the single project`(info) => info.asQuads.map(Delete).toList.pure[F].widen
    case `DS exists on TS only along other projects - no info visibility change`(topSameAs, link) =>
      deleteLink(topSameAs, link).pure[F].widen
    case `DS exists on TS only along other projects - info visibility change`(topSameAs,
                                                                              oldVisibility,
                                                                              newVisibility,
                                                                              link
        ) =>
      (deleteInfoVisibility(topSameAs, oldVisibility) ::
        insertInfoVisibility(topSameAs, newVisibility) ::
        deleteLink(topSameAs, link)).pure[F].widen
    case infoSet =>
      new IllegalStateException(show"Cannot calculate update commands for $infoSet").raiseError[F, List[UpdateCommand]]
  }

  private object `DS exists on Project only` {
    def unapply(infoSet: CalculatorInfoSet): Option[SearchInfo] = infoSet match {
      case CalculatorInfoSet.ModelInfoOnly(_, modelInfo) => modelInfo.some
      case _                                             => None
    }
  }

  private object `DS exists on Project & TS - no visibility changes` {
    def unapply(infoSet: CalculatorInfoSet): Boolean = infoSet match {
      case CalculatorInfoSet.AllInfos(proj, modelInfo, tsInfo, tsVisibilities) =>
        tsInfo.findLink(proj.resourceId).nonEmpty &&
        maxVisibilityNow(tsVisibilities) == maxVisibilityAfter(tsVisibilities, modelInfo)
      case _ => false
    }
  }

  private object `DS exists on Project & TS but on other narrower visibility Projects` {
    def unapply(
        infoSet: CalculatorInfoSet
    ): Option[(datasets.TopmostSameAs, projects.Visibility, projects.Visibility, Link)] =
      infoSet match {
        case CalculatorInfoSet.AllInfos(proj, modelInfo, tsInfo, tsVisibilities)
            if tsInfo.findLink(proj.resourceId).isEmpty &&
              (findMax(tsVisibilities) < modelInfo.visibility) =>
          (modelInfo.topmostSameAs, tsInfo.visibility, modelInfo.visibility, modelInfo.links.head).some
        case _ => None
      }
  }

  private object `DS exists on Project & TS but on other broader or same visibility Projects` {
    def unapply(infoSet: CalculatorInfoSet): Option[(datasets.TopmostSameAs, Link)] = infoSet match {
      case CalculatorInfoSet.AllInfos(proj, modelInfo, tsInfo, tsVisibilities)
          if tsInfo.findLink(proj.resourceId).isEmpty &&
            (findMax(tsVisibilities) >= modelInfo.visibility) =>
        (modelInfo.topmostSameAs, modelInfo.links.head).some
      case _ => None
    }
  }

  object `DS exists on Project & TS - visibility change affects TS` {
    def unapply(
        infoSet: CalculatorInfoSet
    ): Option[(datasets.TopmostSameAs, projects.Visibility, projects.Visibility)] = infoSet match {
      case CalculatorInfoSet.AllInfos(proj, modelInfo, tsInfo, tsVisibilities)
          if tsInfo.findLink(proj.resourceId).nonEmpty &&
            maxVisibilityNow(tsVisibilities) != maxVisibilityAfter(tsVisibilities, modelInfo) =>
        (modelInfo.topmostSameAs, tsInfo.visibility, modelInfo.visibility).some
      case _ => None
    }
  }

  private object `DS exists on TS only on the single project` {
    def unapply(infoSet: CalculatorInfoSet): Option[SearchInfo] = infoSet match {
      case CalculatorInfoSet.TSInfoOnly(proj, tsInfo, _)
          if tsInfo.findLink(proj.resourceId).nonEmpty && tsInfo.links.size == 1 =>
        tsInfo.some
      case _ => None
    }
  }

  private object `DS exists on TS only along other projects - no info visibility change` {
    def unapply(infoSet: CalculatorInfoSet): Option[(datasets.TopmostSameAs, Link)] = infoSet match {
      case CalculatorInfoSet.TSInfoOnly(proj, tsInfo, tsVisibilities)
          if tsInfo.findLink(proj.resourceId).nonEmpty &&
            tsInfo.links.size > 1 &&
            maxVisibilityNow(tsVisibilities) == findMax(tsVisibilities - proj.resourceId) =>
        tsInfo.findLink(proj.resourceId).map(tsInfo.topmostSameAs -> _)
      case _ => None
    }
  }

  private object `DS exists on TS only along other projects - info visibility change` {
    def unapply(
        infoSet: CalculatorInfoSet
    ): Option[(datasets.TopmostSameAs, projects.Visibility, projects.Visibility, Link)] = infoSet match {
      case CalculatorInfoSet.TSInfoOnly(proj, tsInfo, tsVisibilities)
          if tsInfo.findLink(proj.resourceId).nonEmpty &&
            tsInfo.links.size > 1 &&
            maxVisibilityNow(tsVisibilities) != findMax(tsVisibilities - proj.resourceId) =>
        tsInfo
          .findLink(proj.resourceId)
          .map(link => (tsInfo.topmostSameAs, tsInfo.visibility, findMax(tsVisibilities - proj.resourceId), link))
      case _ => None
    }
  }

  private def maxVisibilityNow(tsVisibilities: Map[projects.ResourceId, projects.Visibility]) =
    findMax(tsVisibilities)

  private def maxVisibilityAfter(tsVisibilities: Map[projects.ResourceId, projects.Visibility], modelInfo: SearchInfo) =
    findMax(tsVisibilities + (modelInfo.links.head.projectId -> modelInfo.visibility))

  private def findMax(visibilities: Map[projects.ResourceId, projects.Visibility]): projects.Visibility =
    visibilities.maxBy(_._2)._2

  private def noUpdates = List.empty[UpdateCommand].pure[F]

  private def deleteInfoVisibility(topSameAs: datasets.TopmostSameAs, visibility: projects.Visibility) =
    Delete(visibilityQuad(topSameAs, visibility))

  private def insertInfoVisibility(topSameAs: datasets.TopmostSameAs, visibility: projects.Visibility) =
    Insert(visibilityQuad(topSameAs, visibility))

  private def insertLink(topSameAs: datasets.TopmostSameAs, link: Link) =
    Insert(infoLinkEdge(topSameAs, link.resourceId)) :: link.asQuads.map(Insert).toList

  private def deleteLink(topSameAs: datasets.TopmostSameAs, link: Link) =
    Delete(infoLinkEdge(topSameAs, link.resourceId)) :: link.asQuads.map(Delete).toList

  private def infoLinkEdge(topSameAs: datasets.TopmostSameAs, linkId: links.ResourceId) =
    quad(topSameAs, linkProperty, linkId.asEntityId)

  private def visibilityQuad(topSameAs: datasets.TopmostSameAs, visibility: projects.Visibility) =
    quad(topSameAs, visibilityProperty.id, visibility.asObject)

  private def quad(topSameAs: datasets.TopmostSameAs, property: Property, obj: TripleObject): Quad =
    DatasetsQuad(topSameAs, property, obj)
}

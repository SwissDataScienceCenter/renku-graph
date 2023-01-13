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
import SearchInfoLens.findLink
import SearchInfoOntology.{linkProperty, visibilityProperty}
import UpdateCommand._
import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.entities.Project
import io.renku.graph.model.{datasets, projects}
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.{Quad, TripleObject}
import io.renku.triplesstore.client.syntax._

private object CommandsCalculator {

  def calculateCommands[F[_]: MonadThrow]: CalculatorInfoSet => F[List[UpdateCommand]] = {
    case `DS exists on Project & TS without any other projects`() => noUpdates
    case `DS exists on Project & TS along other Projects`()       => noUpdates
    case `DS exists on Project only`(info)                        => info.asQuads.map(Insert).toList.pure[F].widen
    case `DS exists on Project & DS from TS linked only to other narrower visibility Projects`(topSameAs,
                                                                                               oldVisibility,
                                                                                               newVisibility,
                                                                                               newLink
        ) =>
      (deleteInfoVisibility(topSameAs, oldVisibility) ::
        insertInfoVisibility(topSameAs, newVisibility) ::
        insertLink(topSameAs, newLink)).pure[F]
    case `DS exists on Project & DS from TS linked only to other broader or same visibility Projects`(topSameAs,
                                                                                                      newLink
        ) =>
      insertLink(topSameAs, newLink).pure[F].widen
    case `DS exists on Project & TS - the updated visibility does not change TS info visibility`(
          linkId,
          oldVisibility,
          newVisibility
        ) =>
      (deleteLinkVisibility(linkId, oldVisibility) :: insertLinkVisibility(linkId, newVisibility) :: Nil).pure[F]
    case `DS exists on Project & TS - the updated visibility does change TS info visibility`(
          topSameAs,
          linkId,
          oldVisibility,
          newVisibility
        ) =>
      (deleteInfoVisibility(topSameAs, oldVisibility) ::
        insertInfoVisibility(topSameAs, newVisibility) ::
        deleteLinkVisibility(linkId, oldVisibility) ::
        insertLinkVisibility(linkId, newVisibility) :: Nil).pure[F]
    case `DS exists on TS only on the single project`(info) => info.asQuads.map(Delete).toList.pure[F].widen
    case `DS present on TS only on many projects all with broader or same visibility`(topSameAs, link) =>
      deleteLink(topSameAs, link).pure[F].widen
    case `DS present on TS only on many projects all with narrower visibility`(topSameAs, link, newVisibility) =>
      (deleteInfoVisibility(topSameAs, link.visibility) ::
        insertInfoVisibility(topSameAs, newVisibility) ::
        deleteLink(topSameAs, link)).pure[F].widen
    case infoSet =>
      new IllegalStateException(show"Cannot calculate update commands for $infoSet").raiseError[F, List[UpdateCommand]]
  }

  private object `DS exists on Project & TS without any other projects` {
    def unapply(infoSet: CalculatorInfoSet): Boolean = infoSet match {
      case CalculatorInfoSet(_, Some(modelInfo), Some(tsInfo)) => modelInfo == tsInfo
      case _                                                   => false
    }
  }

  private object `DS exists on Project & TS along other Projects` {
    def unapply(infoSet: CalculatorInfoSet): Boolean = infoSet match {
      case CalculatorInfoSet(_, Some(modelInfo), Some(tsInfo)) =>
        tsInfo.links.size > 1 &&
        (findProjectLink(infoSet.project)(modelInfo) -> findProjectLink(infoSet.project)(tsInfo))
          .mapN(_ == _)
          .getOrElse(false)
      case _ => false
    }
  }

  private object `DS exists on Project only` {
    def unapply(infoSet: CalculatorInfoSet): Option[SearchInfo] = infoSet match {
      case CalculatorInfoSet(_, someModelInfo @ Some(_), None) => someModelInfo
      case _                                                   => None
    }
  }

  private object `DS exists on Project & DS from TS linked only to other narrower visibility Projects` {
    def unapply(
        infoSet: CalculatorInfoSet
    ): Option[(datasets.TopmostSameAs, projects.Visibility, projects.Visibility, Link)] =
      infoSet match {
        case CalculatorInfoSet(_, Some(modelInfo), Some(tsInfo)) =>
          findProjectLink(infoSet.project)(tsInfo) match {
            case None if tsInfo.visibility < modelInfo.visibility =>
              Some(tsInfo.topmostSameAs, tsInfo.visibility, modelInfo.visibility, modelInfo.links.head)
            case _ => None
          }
        case _ => None
      }
  }

  object `DS exists on Project & DS from TS linked only to other broader or same visibility Projects` {
    def unapply(
        infoSet: CalculatorInfoSet
    ): Option[(datasets.TopmostSameAs, Link)] =
      infoSet match {
        case CalculatorInfoSet(_, Some(modelInfo), Some(tsInfo)) =>
          findProjectLink(infoSet.project)(tsInfo) match {
            case None if tsInfo.visibility >= modelInfo.visibility => Some(tsInfo.topmostSameAs, modelInfo.links.head)
            case _                                                 => None
          }
        case _ => None
      }
  }

  object `DS exists on Project & TS - the updated visibility does not change TS info visibility` {
    def unapply(
        infoSet: CalculatorInfoSet
    ): Option[(links.ResourceId, projects.Visibility, projects.Visibility)] =
      infoSet match {
        case CalculatorInfoSet(_, Some(modelInfo), Some(tsInfo)) =>
          (findProjectLink(infoSet.project)(modelInfo) -> findProjectLink(infoSet.project)(tsInfo)).flatMapN {
            case (modelLink, tsLink) if modelLink.visibility == tsLink.visibility => None
            case (modelLink, tsLink) =>
              findOtherProjectLinks(infoSet.project)(tsInfo) match {
                case Nil =>
                  None
                case otherProjectsLinks if maxVisibility(otherProjectsLinks) >= modelInfo.visibility =>
                  (tsLink.resourceId, tsLink.visibility, modelLink.visibility).some
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }
  }

  object `DS exists on Project & TS - the updated visibility does change TS info visibility` {
    def unapply(
        infoSet: CalculatorInfoSet
    ): Option[(datasets.TopmostSameAs, links.ResourceId, projects.Visibility, projects.Visibility)] =
      infoSet match {
        case CalculatorInfoSet(_, Some(modelInfo), Some(tsInfo)) =>
          (findProjectLink(infoSet.project)(modelInfo) -> findProjectLink(infoSet.project)(tsInfo)).flatMapN {
            case (modelLink, tsLink) if modelLink.visibility == tsLink.visibility => None
            case (modelLink, tsLink) =>
              findOtherProjectLinks(infoSet.project)(tsInfo) match {
                case Nil =>
                  (tsInfo.topmostSameAs, tsLink.resourceId, tsLink.visibility, modelLink.visibility).some
                case otherProjectsLinks if maxVisibility(otherProjectsLinks) < modelInfo.visibility =>
                  (tsInfo.topmostSameAs, tsLink.resourceId, tsLink.visibility, modelLink.visibility).some
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }
  }

  private object `DS exists on TS only on the single project` {
    def unapply(infoSet: CalculatorInfoSet): Option[SearchInfo] = infoSet match {
      case CalculatorInfoSet(_, None, Some(tsInfo))
          if tsInfo.links.size == 1 && findProjectLink(infoSet.project)(tsInfo).nonEmpty =>
        findLink(infoSet.project.resourceId)(tsInfo).map(_ => tsInfo)
      case _ => None
    }
  }

  private object `DS present on TS only on many projects all with broader or same visibility` {
    def unapply(infoSet: CalculatorInfoSet): Option[(datasets.TopmostSameAs, Link)] = infoSet match {
      case CalculatorInfoSet(_, None, Some(tsInfo)) =>
        findOtherProjectLinks(infoSet.project)(tsInfo) match {
          case Nil => None
          case otherProjLinks =>
            findProjectLink(infoSet.project)(tsInfo).flatMap(projLink =>
              if (maxVisibility(otherProjLinks) >= projLink.visibility) (tsInfo.topmostSameAs -> projLink).some
              else None
            )
        }
      case _ => None
    }
  }

  private object `DS present on TS only on many projects all with narrower visibility` {
    def unapply(infoSet: CalculatorInfoSet): Option[(datasets.TopmostSameAs, Link, projects.Visibility)] =
      infoSet match {
        case CalculatorInfoSet(_, None, Some(tsInfo)) =>
          findOtherProjectLinks(infoSet.project)(tsInfo) match {
            case Nil => None
            case otherProjLinks =>
              findProjectLink(infoSet.project)(tsInfo).flatMap { projLink =>
                val newInfoVisibility = maxVisibility(otherProjLinks)
                if (newInfoVisibility < projLink.visibility)
                  (tsInfo.topmostSameAs, projLink, newInfoVisibility).some
                else None
              }
          }
        case _ => None
      }
  }

  private def findProjectLink(project: Project): SearchInfo => Option[Link] =
    findLink(project.resourceId)

  private def findOtherProjectLinks(project: Project): SearchInfo => List[Link] =
    _.links.filterNot(_.projectId == project.resourceId)

  private lazy val maxVisibility: List[Link] => projects.Visibility = _.map(_.visibility).max

  private def noUpdates[F[_]: MonadThrow] = List.empty[UpdateCommand].pure[F]

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

  private def insertLinkVisibility(linkId: links.ResourceId, visibility: projects.Visibility) =
    Insert(linkVisibilityQuad(linkId, visibility))

  private def deleteLinkVisibility(linkId: links.ResourceId, visibility: projects.Visibility) =
    Delete(linkVisibilityQuad(linkId, visibility))

  private def linkVisibilityQuad(linkId: links.ResourceId, visibility: projects.Visibility) =
    DatasetsQuad(linkId, visibilityProperty.id, visibility.asObject)

  private def visibilityQuad(topSameAs: datasets.TopmostSameAs, visibility: projects.Visibility) =
    quad(topSameAs, visibilityProperty.id, visibility.asObject)

  private def quad(topSameAs: datasets.TopmostSameAs, property: Property, obj: TripleObject): Quad =
    DatasetsQuad(topSameAs, property, obj)
}

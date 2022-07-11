/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.lineage

import cats.MonadThrow
import cats.syntax.all._
import io.renku.jsonld.EntityId
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.knowledgegraph.lineage.model._

private trait EdgesTrimmer[F[_]] {
  def trim(edges: EdgeMap, location: Location): F[EdgeMap]
}

/** Removes graphs which are not connected to the given location of the dataset (not workflow) which are rectangles.
  */
private class EdgesTrimmerImpl[F[_]: MonadThrow]() extends EdgesTrimmer[F] {

  /** @param edges
    *   Edges from the whole project
    * @param location
    *   location of file the user selected in the UI
    * @return
    *   Trimmed graph with only nodes connected to the location
    */
  def trim(edges: EdgeMap, location: Location): F[EdgeMap] = {

    val latestEdges = removeEdgesWithOverriddenTOs(edges)

    List(
      findEdgesConnected[From](to = Set(location), _: EdgeMap),
      findEdgesConnected[To](to = Set(location), _: EdgeMap)
    ).flatMap(_.apply(latestEdges)).toMap.pure[F]
  }

  private def removeEdgesWithOverriddenTOs(edges: EdgeMap): EdgeMap =
    removeEdgesWithOverriddenTOs(nodesToCheck = collectAllTOs(edges), edgesToCheck = edges)

  private def collectAllTOs(edges: EdgeMap): Set[Location] =
    edges.foldLeft(Set.empty[Location]) { case (allLocations, (_, (_, to))) => allLocations ++ to }

  @scala.annotation.tailrec
  private def removeEdgesWithOverriddenTOs(nodesToCheck: Set[Location],
                                           edgesToCheck: EdgeMap,
                                           latestEdges:  EdgeMap = Map.empty
  ): EdgeMap = nodesToCheck.headOption match {
    case None => latestEdges
    case Some(location) =>
      edgesToCheck
        .filter(edgesProducing(location))
        .toSeq
        .sortBy(runInfoDate)
        .reverse match {
        case Nil =>
          removeEdgesWithOverriddenTOs(nodesToCheck.tail, edgesToCheck, latestEdges)
        case latestEdge :: earlierEdges =>
          removeEdgesWithOverriddenTOs(nodesToCheck.tail,
                                       edgesToCheck.removedAll(earlierEdges.map(_._1)),
                                       latestEdges + latestEdge
          )
      }
  }

  private def edgesProducing(location: Location): ((ExecutionInfo, FromAndToNodes)) => Boolean = { case (_, (_, to)) =>
    to contains location
  }

  private lazy val runInfoDate: ((ExecutionInfo, FromAndToNodes)) => RunDate = { case (ExecutionInfo(_, date), _) =>
    date
  }

  @scala.annotation.tailrec
  private def findEdgesConnected[T <: TraversalDirection](
      to:                       Set[Location],
      edgesToCheck:             EdgeMap,
      foundEdges:               EdgeMap = Map.empty
  )(implicit traversalStrategy: TraversalStrategy[T]): EdgeMap = {
    import traversalStrategy._

    to.headOption match {
      case None => foundEdges
      case Some(nodeToCheck) =>
        edgesToCheck partition edgeContaining(nodeToCheck) match {
          case (foundEdgesWithLocation, remainingEdges) if foundEdgesWithLocation.isEmpty =>
            findEdgesConnected[T](to.tail, remainingEdges, foundEdges)
          case (foundEdgesWithLocation, remainingEdges) =>
            val locationsToCheck = foundEdgesWithLocation.collect(locationsOtherThan(nodeToCheck)).flatten
            findEdgesConnected[T](
              to.tail ++ locationsToCheck,
              edgesToCheck = remainingEdges ++ reAdd[T](foundEdgesWithLocation,
                                                        whenEdgeContains = to.tail.headOption,
                                                        without = nodeToCheck
              ),
              foundEdges combine removeSiblings(foundEdgesWithLocation, nodeToCheck)
            )
        }
    }
  }

  private def reAdd[T <: TraversalDirection](edgeMap: EdgeMap, whenEdgeContains: Option[Location], without: Location)(
      implicit traversalStrategy:                     TraversalStrategy[T]
  ): EdgeMap = {
    import traversalStrategy._

    whenEdgeContains match {
      case Some(location) => removeNode(edgeMap filter edgeContaining(location), without)
      case None           => Map.empty
    }
  }

  private sealed trait TraversalDirection
  private case object To   extends TraversalDirection
  private case object From extends TraversalDirection
  private type To   = To.type
  private type From = From.type

  private trait TraversalStrategy[T <: TraversalDirection] {

    def removeSiblings(edges: EdgeMap, nodeToCheck: Location): EdgeMap

    def removeNode(edges: EdgeMap, nodeToRemove: Location): EdgeMap

    def edgeContaining(location: Location): ((ExecutionInfo, FromAndToNodes)) => Boolean

    def locationsOtherThan(location: Location): PartialFunction[(ExecutionInfo, FromAndToNodes), Set[Location]]
  }

  private implicit lazy val toTraversalStrategy: TraversalStrategy[To] = new TraversalStrategy[To] {

    def removeNode(edges: EdgeMap, nodeToRemove: Location): EdgeMap =
      edges.foldLeft(Map.empty: EdgeMap) { case (acc, (runInfo, (from, to))) =>
        acc + (runInfo -> (from.filterNot(_ == nodeToRemove), to))
      }

    def removeSiblings(edges: EdgeMap, nodeToCheck: Location): EdgeMap =
      edges.foldLeft(Map.empty: EdgeMap) {
        case (graphWithoutSiblings, entry @ (_, (from, to)))
            if from.contains(nodeToCheck) && to.contains(nodeToCheck) =>
          graphWithoutSiblings + entry
        case (graphWithoutSiblings, (runInfo, (from, to))) if from.contains(nodeToCheck) =>
          graphWithoutSiblings + (runInfo -> (Set(nodeToCheck), to))
        case (graphWithoutSiblings, _) => graphWithoutSiblings
      }

    def edgeContaining(location: Location): ((ExecutionInfo, FromAndToNodes)) => Boolean = {
      case (_, (from, _)) if from contains location => true
      case _                                        => false
    }

    def locationsOtherThan(location: Location): PartialFunction[(ExecutionInfo, FromAndToNodes), Set[Location]] = {
      case (_, (from, to)) if from contains location => to
    }
  }

  private implicit lazy val fromTraversalStrategy: TraversalStrategy[From] = new TraversalStrategy[From] {

    def removeNode(edges: EdgeMap, nodeToRemove: Location): EdgeMap =
      edges.foldLeft(Map.empty: EdgeMap) { case (acc, (runInfo, (from, to))) =>
        acc + (runInfo -> (from, to.filterNot(_ == nodeToRemove)))
      }

    def removeSiblings(edges: EdgeMap, nodeToCheck: Location): EdgeMap =
      edges.foldLeft(Map.empty: EdgeMap) {
        case (graphWithoutSiblings, entry @ (_, (from, to)))
            if from.contains(nodeToCheck) && to.contains(nodeToCheck) =>
          graphWithoutSiblings + entry
        case (graphWithoutSiblings, (runInfo, (from, to))) if to.contains(nodeToCheck) =>
          graphWithoutSiblings + (runInfo -> (from, Set(nodeToCheck)))
        case (graphWithoutSiblings, _) => graphWithoutSiblings
      }

    def edgeContaining(location: Location): ((ExecutionInfo, FromAndToNodes)) => Boolean = {
      case (_, (_, to)) if to contains location => true
      case _                                    => false
    }

    def locationsOtherThan(location: Location): PartialFunction[(ExecutionInfo, FromAndToNodes), Set[Location]] = {
      case (_, (from, to)) if to contains location => from
    }
  }
}

private object LineageDataTrimmer {
  def apply[F[_]: MonadThrow]: F[EdgesTrimmer[F]] = MonadThrow[F].catchNonFatal(new EdgesTrimmerImpl[F]())

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val toLocation: Node.Location = Node.Location(entityId.value.toString)
  }
}

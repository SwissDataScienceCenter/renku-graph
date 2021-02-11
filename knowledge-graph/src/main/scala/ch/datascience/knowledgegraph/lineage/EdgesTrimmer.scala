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

package ch.datascience.knowledgegraph.lineage

import cats.MonadError
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import ch.datascience.knowledgegraph.lineage.model.{EdgeMap, Node}
import io.renku.jsonld.EntityId

private trait EdgesTrimmer[Interpretation[_]] {
  def trim(edges: EdgeMap, location: Location): Interpretation[EdgeMap]
}

/**
  * Removes graphs which are not connected to the given location of the dataset (not workflow) which are rectangles.
  */
private class EdgesTrimmerImpl[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable])
    extends EdgesTrimmer[Interpretation] {

  /**
    * @param edges Edges from the whole project
    * @param location location of file the user selected in the UI
    * @return Trimmed graph with only nodes connected to the location
    */
  def trim(edges: EdgeMap, location: Location): Interpretation[EdgeMap] =
    findEdgesConnected[To](to = Set(location), edgesLeft = edges)
      .combine(findEdgesConnected[From](to = Set(location), edgesLeft = edges))
      .pure[Interpretation]

  @scala.annotation.tailrec
  private def findEdgesConnected[T <: TraversalDirection](
      to:                       Set[Location],
      edgesLeft:                Map[EntityId, FromAndToNodes],
      foundEdges:               Map[EntityId, FromAndToNodes] = Map.empty
  )(implicit traversalStrategy: TraversalStrategy[T]): Map[EntityId, FromAndToNodes] = {
    import traversalStrategy._
    to.headOption match {
      case None => foundEdges
      case Some(nodeToCheck) =>
        edgesLeft.partition(edgeWith(nodeToCheck)) match {
          case (foundEdgesWithLocation, remainingEdges) if foundEdgesWithLocation.isEmpty =>
            findEdgesConnected[T](to.tail, remainingEdges, foundEdges)
          case (foundEdgesWithLocation, remainingEdges) =>
            val locationsToCheck = foundEdgesWithLocation.collect(locationsOtherThan(nodeToCheck)).flatten
            findEdgesConnected[T](to.tail ++ locationsToCheck,
                                  remainingEdges,
                                  foundEdges ++ removeSiblings(foundEdgesWithLocation, nodeToCheck)
            )
        }
    }
  }

  private type FromAndToNodes = (Set[Node.Location], Set[Node.Location])

  private sealed trait TraversalDirection
  private case object To   extends TraversalDirection
  private case object From extends TraversalDirection
  private type To   = To.type
  private type From = From.type

  private trait TraversalStrategy[T <: TraversalDirection] {
    def removeSiblings(edges:       Map[EntityId, FromAndToNodes],
                       nodeToCheck: Location
    ): Map[EntityId, (Set[Location], Set[Location])]

    def edgeWith(location: Location): ((EntityId, FromAndToNodes)) => Boolean

    def locationsOtherThan(
        location: Location
    ): PartialFunction[(EntityId, FromAndToNodes), Set[Location]]
  }

  private implicit lazy val toTraversalStrategy: TraversalStrategy[To] = new TraversalStrategy[To] {
    def removeSiblings(edges: Map[EntityId, FromAndToNodes], nodeToCheck: Location): Map[EntityId, FromAndToNodes] =
      edges.foldLeft(Map.empty[EntityId, FromAndToNodes]) {
        case (graphWithoutSiblings, entry @ (_, (from, to)))
            if from.contains(nodeToCheck) && to.contains(nodeToCheck) =>
          graphWithoutSiblings + entry
        case (graphWithoutSiblings, (entityId, (from, to))) if from.contains(nodeToCheck) =>
          graphWithoutSiblings + (entityId -> (Set(nodeToCheck), to))
        case (graphWithoutSiblings, _) => graphWithoutSiblings
      }

    def edgeWith(location: Location): ((EntityId, FromAndToNodes)) => Boolean = {
      case (_, (from, _)) if from.contains(location) => true
      case _                                         => false
    }

    def locationsOtherThan(
        location: Location
    ): PartialFunction[(EntityId, FromAndToNodes), Set[Location]] = {
      case (_, (from, to)) if from.contains(location) => to
    }
  }

  private implicit lazy val fromTraversalStrategy: TraversalStrategy[From] = new TraversalStrategy[From] {
    def removeSiblings(edges:       Map[EntityId, FromAndToNodes],
                       nodeToCheck: Location
    ): Map[EntityId, (Set[Location], Set[Location])] =
      edges.foldLeft(Map.empty[EntityId, FromAndToNodes]) {
        case (graphWithoutSiblings, entry @ (_, (from, to)))
            if from.contains(nodeToCheck) && to.contains(nodeToCheck) =>
          graphWithoutSiblings + entry
        case (graphWithoutSiblings, (entityId, (from, to))) if to.contains(nodeToCheck) =>
          graphWithoutSiblings + (entityId -> (from, Set(nodeToCheck)))
        case (graphWithoutSiblings, _) => graphWithoutSiblings
      }

    def edgeWith(location: Location): ((EntityId, FromAndToNodes)) => Boolean = {
      case (_, (_, to)) if to.contains(location) => true
      case _                                     => false
    }

    def locationsOtherThan(
        location: Location
    ): PartialFunction[(EntityId, FromAndToNodes), Set[Location]] = {
      case (_, (from, to)) if to.contains(location) => from
    }
  }

}

private object IOLineageDataTrimmer {
  def apply(): IO[EdgesTrimmer[IO]] = new EdgesTrimmerImpl[IO]().pure[IO]

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val toLocation: Node.Location = Node.Location(entityId.value.toString)
  }
}

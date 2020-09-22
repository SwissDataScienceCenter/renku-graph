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

package ch.datascience.knowledgegraph.lineage

import cats.MonadError
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.knowledgegraph.lineage.IOLineageDataTrimmer.EntityIdOps
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import ch.datascience.knowledgegraph.lineage.model.{EdgeMap, Node}
import io.renku.jsonld.EntityId

import scala.language.higherKinds

private trait EdgesTrimmer[Interpretation[_]] {
  def trim(edges: EdgeMap, location: Location): Interpretation[EdgeMap]
}

/**
  * Removes graphs which are not connected to the given location of the dataset (not workflow) which are rectangles.
  * @param ME
  * @tparam Interpretation
  */
private class EdgesTrimmerImpl[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable])
    extends EdgesTrimmer[Interpretation] {

  /**
    * @param edges Edges from the whole project
    * @param location location of file the user selected in the UI
    * @return Trimmed graph with only nodes connected to the location
    */
  def trim(edges: EdgeMap, location: Location): Interpretation[EdgeMap] =
    findEdgesConnected(to = Set(location), edgesLeft = edges).pure[Interpretation]

  @scala.annotation.tailrec
  private def findEdgesConnected(
      to:         Set[Location],
      edgesLeft:  Map[EntityId, (Set[Node.Location], Set[Node.Location])],
      foundEdges: Map[EntityId, (Set[Node.Location], Set[Node.Location])] = Map.empty
  ): Map[EntityId, (Set[Node.Location], Set[Node.Location])] =
    to.headOption match {
      case None => foundEdges
      case Some(nodeToCheck) =>
        edgesLeft.partition(edgeWith(nodeToCheck)) match {
          case (foundEdgesWithLocation, remainingEdges) if foundEdgesWithLocation.isEmpty =>
            findEdgesConnected(to.tail, remainingEdges, foundEdges)
          case (foundEdgesWithLocation, remainingEdges) =>
            val locationsToCheck = foundEdgesWithLocation.collect(locationsOtherThan(nodeToCheck)).flatten
            findEdgesConnected(to.tail ++ locationsToCheck, remainingEdges, foundEdges ++ foundEdgesWithLocation)
        }
    }

  private def edgeWith(location: Location): ((EntityId, (Set[Node.Location], Set[Node.Location]))) => Boolean = {
    case (runPlanId, (_, _)) if runPlanId.toLocation == location             => true
    case (_, (from, to)) if from.contains(location) || to.contains(location) => true
    case _                                                                   => false
  }

  private def locationsOtherThan(
      location: Location
  ): PartialFunction[(EntityId, (Set[Node.Location], Set[Node.Location])), Set[Location]] = {
    case (runPlanId, (from, to)) if runPlanId.toLocation != location =>
      Set(runPlanId.toLocation) ++ from.filter(_ != location) ++ to.filter(_ != location)
    case (_, (from, to)) => from.filter(_ != location) ++ to.filter(_ != location)
  }
}

private object IOLineageDataTrimmer {
  def apply(): IO[EdgesTrimmer[IO]] = new EdgesTrimmerImpl[IO]().pure[IO]

  implicit class EntityIdOps(entityId: EntityId) {
    lazy val toLocation: Node.Location = Node.Location(entityId.value.toString)
  }
}

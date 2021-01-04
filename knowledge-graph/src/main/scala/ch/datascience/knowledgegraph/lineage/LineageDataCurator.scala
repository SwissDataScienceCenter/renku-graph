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
import cats.data.OptionT
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import ch.datascience.knowledgegraph.lineage.model.{Edge, Lineage, Node}

trait LineageDataCurator[Interpretation[_]] {
  def curate(lineage: Lineage, location: Location): OptionT[Interpretation, Lineage]
}

private class LineageDataCuratorImpl[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable])
    extends LineageDataCurator[Interpretation] {

  def curate(lineage: Lineage, location: Location): OptionT[Interpretation, Lineage] =
    findEdgesConnected(to = Set(location), edgesLeft = lineage.edges) match {
      case edges if edges.isEmpty => OptionT.none[Interpretation, Lineage]
      case edges =>
        OptionT.liftF {
          for {
            nodes   <- collectNodes(edges, lineage)
            lineage <- Lineage.from[Interpretation](edges, nodes)
          } yield lineage
        }
    }

  @scala.annotation.tailrec
  private def findEdgesConnected(to:         Set[Location],
                                 edgesLeft:  Set[Edge],
                                 foundEdges: Set[Edge] = Set.empty
  ): Set[Edge] =
    to.headOption match {
      case None => foundEdges
      case Some(nodeToCheck) =>
        edgesLeft.partition(edgeWith(nodeToCheck)) match {
          case (foundEdgesWithLocation, remainingEdges) if foundEdgesWithLocation.isEmpty =>
            findEdgesConnected(to.tail, remainingEdges, foundEdges)
          case (foundEdgesWithLocation, remainingEdges) =>
            val locationsToCheck = foundEdgesWithLocation.collect(locationsOtherThan(nodeToCheck))
            findEdgesConnected(to.tail ++ locationsToCheck, remainingEdges, foundEdges ++ foundEdgesWithLocation)
        }
    }

  private def collectNodes(from: Set[Edge], lineage: Lineage) =
    from.foldLeft(Set.empty[Node].pure[Interpretation]) { case (allNodes, Edge(source, target)) =>
      (lineage.getNode(source) -> lineage.getNode(target))
        .mapN { case (sourceNode, targetNode) => allNodes.map(_ + sourceNode + targetNode) }
        .getOrElse {
          new IllegalStateException(s"There is no node for either source $source or target $target")
            .raiseError[Interpretation, Set[Node]]
        }
    }

  private def edgeWith(location: Location): Edge => Boolean = {
    case Edge(`location`, _) => true
    case Edge(_, `location`) => true
    case _                   => false
  }

  private def locationsOtherThan(location: Location): PartialFunction[Edge, Location] = {
    case Edge(`location`, target) => target
    case Edge(source, `location`) => source
  }
}

object IOLineageDataCurator {
  def apply(): IO[LineageDataCurator[IO]] = new LineageDataCuratorImpl[IO]().pure[IO]
}

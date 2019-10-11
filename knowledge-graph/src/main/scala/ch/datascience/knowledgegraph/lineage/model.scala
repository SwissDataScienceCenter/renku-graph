/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import model.Node.{SourceNode, TargetNode}

import scala.language.higherKinds

object model {

  final case class Lineage private (edges: Set[Edge], nodes: Set[Node])

  object Lineage {

    def from[Interpretation[_]](edges: Set[Edge], nodes: Set[Node])(
        implicit ME:                   MonadError[Interpretation, Throwable]): Interpretation[Lineage] = {
      val edgesNodes = collectNodes(edges)
      if (edgesNodes == nodes) ME.pure(Lineage(edges, nodes))
      else if ((nodes diff edgesNodes).nonEmpty) ME.raiseError(new IllegalArgumentException("There are orphan nodes"))
      else ME.raiseError(new IllegalArgumentException("There are edges with no nodes definitions"))
    }

    private def collectNodes(edges: Set[Edge]): Set[Node] =
      edges.foldLeft(Set.empty[Node]) { (nodes, edge) =>
        nodes + edge.source + edge.target
      }
  }

  final case class Edge(source: SourceNode, target: TargetNode)

  sealed case class Node(id: NodeId, label: NodeLabel)

  object Node {
    sealed trait SourceNode extends Node

    object SourceNode {
      def apply(id: NodeId, label: NodeLabel): SourceNode = new Node(id, label) with SourceNode
    }

    sealed trait TargetNode extends Node

    object TargetNode {
      def apply(id: NodeId, label: NodeLabel): TargetNode = new Node(id, label) with TargetNode
    }
  }

  final class NodeId private (val value: String) extends AnyVal with StringTinyType
  object NodeId extends TinyTypeFactory[NodeId](new NodeId(_)) with NonBlank

  final class NodeLabel private (val value: String) extends AnyVal with StringTinyType
  object NodeLabel extends TinyTypeFactory[NodeLabel](new NodeLabel(_)) with NonBlank
}

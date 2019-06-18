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

package ch.datascience.graphservice.graphql.lineage

import java.io.Serializable

import ch.datascience.graphservice.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}

object model {

  final case class Lineage(nodes: Set[Node], edges: Set[Edge])

  sealed trait Node extends Product with Serializable {
    val id:    NodeId
    val label: NodeLabel

    override def equals(other: Any): Boolean =
      if (!other.isInstanceOf[Node]) false
      else this.id == other.asInstanceOf[Node].id && this.label == other.asInstanceOf[Node].label
  }

  final class NodeId private (val value: String) extends AnyVal with TinyType[String]
  object NodeId extends TinyTypeFactory[String, NodeId](new NodeId(_)) with NonBlank

  final class NodeLabel private (val value: String) extends AnyVal with TinyType[String]
  object NodeLabel extends TinyTypeFactory[String, NodeLabel](new NodeLabel(_)) with NonBlank

  object Node {
    final case class SourceNode(id: NodeId, label: NodeLabel) extends Node
    final case class TargetNode(id: NodeId, label: NodeLabel) extends Node
  }

  final case class Edge(source: SourceNode, target: TargetNode)
}

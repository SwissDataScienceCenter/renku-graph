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

import ch.datascience.generators.Generators._
import ch.datascience.graphservice.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.graphservice.graphql.lineage.model.{Edge, Node, NodeId, NodeLabel}
import org.scalacheck.{Arbitrary, Gen}

object LineageGenerators {

  implicit val nodeIds:    Gen[NodeId]    = nonEmptyStrings() map NodeId.apply
  implicit val nodeLabels: Gen[NodeLabel] = nonEmptyStrings() map NodeLabel.apply

  implicit val sourceNodes: Gen[SourceNode] = for {
    id    <- nodeIds
    label <- nodeLabels
  } yield SourceNode(id, label)

  implicit val targetNodes: Gen[TargetNode] = for {
    id    <- nodeIds
    label <- nodeLabels
  } yield TargetNode(id, label)

  implicit val nodes: Gen[Node] = for {
    source <- Arbitrary.arbBool.arbitrary
    node   <- if (source) sourceNodes else targetNodes
  } yield node

  implicit val edges: Gen[Edge] = for {
    sourceNode <- sourceNodes
    targetNode <- targetNodes
  } yield Edge(sourceNode, targetNode)
}

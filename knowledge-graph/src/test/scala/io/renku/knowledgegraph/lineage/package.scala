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

package io.renku.knowledgegraph

import cats.syntax.all._
import io.renku.graph.model.entities.{Activity, Entity}
import io.renku.jsonld.EntityId
import io.renku.knowledgegraph.lineage.model._

import java.time.Instant

package object lineage {

  private[lineage] implicit class LineageOps(lineage: Lineage) {

    lazy val toEdgesMap =
      processRunNodes.foldLeft(Map.empty[ExecutionInfo, (Set[Node.Location], Set[Node.Location])]) {
        case (planWithLocation, node) =>
          planWithLocation + (
            ExecutionInfo(EntityId.of(node.location.toString), Instant.now()) -> (
              lineage.collectSources(of = node) -> lineage.collectTargets(of = node)
            )
          )
      }

    def collectSources(of: Node): Set[Node.Location] =
      lineage.edges.foldLeft(Set.empty[Node.Location]) {
        case (locations, Edge(source, of.location)) => locations + source
        case (locations, _)                         => locations
      }

    def collectTargets(of: Node): Set[Node.Location] =
      lineage.edges.foldLeft(Set.empty[Node.Location]) {
        case (locations, Edge(of.location, target)) => locations + target
        case (locations, _)                         => locations
      }

    lazy val locationNodes: Set[Node] = lineage.nodes.filter { node =>
      Set(Entity.fileEntityTypes, Entity.folderEntityTypes)
        .map(_.toList.map(_.show).toSet)
        .contains(node.types.map(_.show))
    }

    lazy val processRunNodes: Set[Node] = lineage.nodes
      .filter(_.types.map(_.show) === Activity.entityTypes.toList.map(_.show).toSet)
  }
}

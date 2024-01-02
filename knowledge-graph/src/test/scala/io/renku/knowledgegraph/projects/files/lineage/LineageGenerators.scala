/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.files.lineage

import io.renku.generators.Generators._
import model._
import org.scalacheck.Gen

import scala.util.Try

object LineageGenerators {

  implicit val nodeIds:       Gen[Node.Id]       = nonBlankStrings(minLength = 3) map (_.value) map Node.Id.apply
  implicit val nodeLocations: Gen[Node.Location] = relativePaths() map Node.Location.apply
  implicit val nodeLabels:    Gen[Node.Label]    = nonBlankStrings(minLength = 3) map (_.value) map Node.Label.apply
  implicit val nodeTypes:     Gen[Node.Type]     = Gen.oneOf(Node.Type.all)

  val entityNodes: Gen[Node] = for {
    location <- nodeLocations
    label    <- nodeLabels
    typ      <- nodeTypes
  } yield Node(location, label, typ)

  val processRunNodes: Gen[Node] = for {
    location <- nodeLocations
    label    <- nodeLabels
  } yield Node(location, label, Node.Type.ProcessRun)

  implicit val nodes: Gen[Node] = Gen.oneOf(processRunNodes, entityNodes)

  implicit val edges: Gen[Edge] = for {
    sourceNode <- nodeLocations
    targetNode <- nodeLocations
  } yield Edge(sourceNode, targetNode)

  implicit val nodePairs: Gen[(Node, Node)] = for {
    processRunNode <- processRunNodes
    entityNode     <- entityNodes
  } yield (processRunNode, entityNode)

  implicit val lineages: Gen[Lineage] = for {
    nodePairsSet <- nonEmptySet(nodePairs, 2)
  } yield {
    val nodes = nodePairsSet.foldLeft(List.empty[Node]) { case (acc, (source, target)) =>
      acc :+ source :+ target
    }
    val edges = (nodes zip nodes.tail).map { case (left, right) =>
      Edge(left.location, right.location)
    }
    Lineage.from[Try](edges.toSet, nodes.toSet).fold(throw _, identity)
  }
}

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

import scala.collection.Set
import scala.language.higherKinds

object model {

  final case class Lineage private (edges: Set[Edge], nodes: Set[Node])

  object Lineage {

    def from[Interpretation[_]](edges: Set[Edge], nodes: Set[Node])(
        implicit ME:                   MonadError[Interpretation, Throwable]
    ): Interpretation[Lineage] = {
      val allEdgesLocations = collectLocations(edges)
      val allNodesLocations = nodes.map(_.location)
      if (allEdgesLocations == allNodesLocations) ME.pure(Lineage(edges, nodes))
      else if ((allNodesLocations diff allEdgesLocations).nonEmpty)
        ME.raiseError(new IllegalArgumentException("There are orphan nodes"))
      else
        ME.raiseError(new IllegalArgumentException("There are edges with no nodes definitions"))
    }

    private def collectLocations(edges: Set[Edge]): Set[Node.Location] =
      edges.foldLeft(Set.empty[Node.Location]) {
        case (acc, Edge(source, target)) => acc + source + target
      }
  }

  final case class Edge(source: Node.Location, target: Node.Location)

  final case class Node(location: Node.Location, label: Node.Label, types: Set[Node.Type])

  object Node {
    import ch.datascience.tinytypes.constraints.{NonBlank, RelativePath}
    import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

    final class Id private (val value: String) extends AnyVal with StringTinyType
    object Id extends TinyTypeFactory[Id](new Id(_)) with NonBlank

    final class Label private (val value: String) extends AnyVal with StringTinyType
    object Label extends TinyTypeFactory[Label](new Label(_)) with NonBlank

    final class Type private (val value: String) extends AnyVal with StringTinyType
    object Type extends TinyTypeFactory[Type](new Type(_)) with NonBlank

    final class Location private (val value: String) extends AnyVal with StringTinyType
    object Location extends TinyTypeFactory[Location](new Location(_)) with RelativePath

    sealed trait SingleWordType extends Product with Serializable {
      val name: String
      override lazy val toString: String = name
    }

    object SingleWordType {
      case object ProcessRun extends SingleWordType { val name: String = "ProcessRun" }
      case object File       extends SingleWordType { val name: String = "File" }
      case object Directory  extends SingleWordType { val name: String = "Directory" }
    }

    implicit class NodeOps(node: Node) {

      import SingleWordType._

      private lazy val FileTypes = Set("http://www.w3.org/ns/prov#Entity", "http://purl.org/wf4ever/wfprov#Artifact")
      private lazy val DirectoryTypes = Set("http://www.w3.org/ns/prov#Entity",
                                            "http://purl.org/wf4ever/wfprov#Artifact",
                                            "http://www.w3.org/ns/prov#Collection")

      lazy val singleWordType: Either[Exception, SingleWordType] = node.types.map(_.toString) match {
        case types if types contains "http://purl.org/wf4ever/wfprov#ProcessRun" => Right(ProcessRun)
        case types if (DirectoryTypes diff types).isEmpty                        => Right(Directory)
        case types if (FileTypes diff types).isEmpty                             => Right(File)
        case types                                                               => Left(new Exception(s"${types.mkString(", ")} cannot be converted to a NodeType"))
      }
    }
  }
}

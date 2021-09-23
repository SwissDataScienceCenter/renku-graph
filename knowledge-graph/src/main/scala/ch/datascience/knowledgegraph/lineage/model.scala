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

import cats.MonadThrow
import cats.syntax.all._
import ch.datascience.knowledgegraph.lineage.model.Node.Location
import ch.datascience.tinytypes.{InstantTinyType, TinyTypeFactory}
import io.renku.jsonld.{EntityId, EntityType}

import java.time.Instant

object model {

  private[lineage] final case class RunInfo(entityId: EntityId, date: RunDate)
  private[lineage] final class RunDate private (val value: Instant) extends AnyVal with InstantTinyType
  private[lineage] implicit object RunDate extends TinyTypeFactory[RunDate](new RunDate(_))
  private[lineage] type FromAndToNodes = (Set[Node.Location], Set[Node.Location])
  private[lineage] type EdgeMapEntry   = (RunInfo, FromAndToNodes)
  private[lineage] type EdgeMap        = Map[RunInfo, FromAndToNodes]

  final case class Lineage private (edges: Set[Edge], nodes: Set[Node]) extends LineageOps

  object Lineage {

    def from[Interpretation[_]: MonadThrow](edges: Set[Edge], nodes: Set[Node]): Interpretation[Lineage] = {
      val allEdgesLocations = collectLocations(edges)
      val allNodesLocations = nodes.map(_.location)
      if (allEdgesLocations == allNodesLocations) Lineage(edges, nodes).pure[Interpretation]
      else if ((allNodesLocations diff allEdgesLocations).nonEmpty)
        new IllegalArgumentException("There are orphan nodes").raiseError[Interpretation, Lineage]
      else
        new IllegalArgumentException("There are edges with no nodes definitions").raiseError[Interpretation, Lineage]
    }

    private def collectLocations(edges: Set[Edge]): Set[Node.Location] =
      edges.foldLeft(Set.empty[Node.Location]) { case (acc, Edge(source, target)) =>
        acc + source + target
      }
  }

  trait LineageOps {
    self: Lineage =>

    def getNode(location: Location): Option[Node] =
      nodes.find(_.location == location)
  }

  final case class Edge(source: Node.Location, target: Node.Location)

  final case class Node(location: Node.Location, label: Node.Label, types: Set[Node.Type])

  object Node {
    import ch.datascience.tinytypes.constraints.NonBlank
    import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}

    final class Id private (val value: String) extends AnyVal with StringTinyType
    object Id extends TinyTypeFactory[Id](new Id(_)) with NonBlank {
      import ch.datascience.graph.model.views.RdfResource
      import ch.datascience.tinytypes.Renderer

      implicit object RdfResourceRenderer extends Renderer[RdfResource, Id] {
        override def render(value: Id): String = s"<$value>"
      }
    }

    final class Label private (val value: String) extends AnyVal with StringTinyType
    object Label extends TinyTypeFactory[Label](new Label(_)) with NonBlank

    final class Type private (val value: String) extends AnyVal with StringTinyType
    object Type extends TinyTypeFactory[Type](new Type(_)) with NonBlank

    final class Location private (val value: String) extends AnyVal with StringTinyType
    object Location extends TinyTypeFactory[Location](new Location(_))

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
      import ch.datascience.graph.model.entities.{Activity, Entity}

      lazy val singleWordType: Either[Exception, SingleWordType] = node.types.map(t => EntityType.of(t.show)) match {
        case types if Activity.entityTypes.toList.toSet === types     => Right(ProcessRun)
        case types if Entity.folderEntityTypes.toList.toSet === types => Right(Directory)
        case types if Entity.fileEntityTypes.toList.toSet === types   => Right(File)
        case types                                                    => Left(new Exception(s"${types.map(_.show).mkString(", ")} cannot be converted to a NodeType"))
      }
    }
  }
}

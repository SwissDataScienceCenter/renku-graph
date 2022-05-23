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

package io.renku.knowledgegraph.lineage

import cats.MonadThrow
import cats.syntax.all._
import io.renku.jsonld.{EntityId, EntityType}
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}

import java.time.Instant

object model {

  private[lineage] final case class ExecutionInfo(entityId: EntityId, date: RunDate)

  private[lineage] trait RunDate extends Any with InstantTinyType

  private[lineage] final case class RunDateImpl(value: Instant) extends AnyVal with RunDate
  private[lineage] implicit object RunDate                      extends TinyTypeFactory[RunDate](RunDateImpl)
  private[lineage] type FromAndToNodes = (Set[Node.Location], Set[Node.Location])
  private[lineage] type EdgeMapEntry   = (ExecutionInfo, FromAndToNodes)
  private[lineage] type EdgeMap        = Map[ExecutionInfo, FromAndToNodes]

  final case class Lineage private (edges: Set[Edge], nodes: Set[Node]) extends LineageOps

  object Lineage {

    def from[F[_]: MonadThrow](edges: Set[Edge], nodes: Set[Node]): F[Lineage] = {
      val allEdgesLocations = collectLocations(edges)
      val allNodesLocations = nodes.map(_.location)
      if (allEdgesLocations == allNodesLocations) Lineage(edges, nodes).pure[F]
      else if ((allNodesLocations diff allEdgesLocations).nonEmpty)
        new IllegalArgumentException("There are orphan nodes").raiseError[F, Lineage]
      else
        new IllegalArgumentException("There are edges with no nodes definitions").raiseError[F, Lineage]
    }

    private def collectLocations(edges: Set[Edge]): Set[Node.Location] =
      edges.foldLeft(Set.empty[Node.Location]) { case (acc, Edge(source, target)) =>
        acc + source + target
      }
  }

  trait LineageOps {
    self: Lineage =>

    def getNode(location: Location): Option[Node] = nodes.find(_.location == location)
  }

  final case class Edge(source: Node.Location, target: Node.Location)

  final case class Node(location: Node.Location, label: Node.Label, types: Set[Node.Type])

  object Node {
    import io.renku.tinytypes.constraints.NonBlank
    import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

    sealed trait Id extends Any with StringTinyType

    private[lineage] final case class IdImpl(value: String) extends AnyVal with Id
    object Id extends TinyTypeFactory[Id](IdImpl) with NonBlank[Id] {

      import io.renku.graph.model.views.RdfResource
      import io.renku.tinytypes.Renderer

      implicit object RdfResourceRenderer extends Renderer[RdfResource, Id] {
        override def render(value: Id): String = s"<$value>"
      }
    }

    sealed trait Label extends Any with StringTinyType

    private[lineage] final case class LabelImpl private (value: String) extends AnyVal with Label
    object Label extends TinyTypeFactory[Label](LabelImpl) with NonBlank[Label]

    sealed trait Type extends Any with StringTinyType

    private[lineage] final case class TypeImpl private (value: String) extends AnyVal with Type
    object Type extends TinyTypeFactory[Type](TypeImpl) with NonBlank[Type]

    sealed trait Location extends Any with StringTinyType

    private[lineage] final case class LocationImpl private (value: String) extends AnyVal with Location
    object Location extends TinyTypeFactory[Location](LocationImpl) {
      def unapply(value: String): Option[Location] = Location.from(value).toOption
    }

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
      import io.renku.graph.model.entities.{Activity, Entity}

      lazy val singleWordType: Either[Exception, SingleWordType] = node.types.map(t => EntityType.of(t.show)) match {
        case types if Activity.entityTypes.toList.toSet === types     => Right(ProcessRun)
        case types if Entity.folderEntityTypes.toList.toSet === types => Right(Directory)
        case types if Entity.fileEntityTypes.toList.toSet === types   => Right(File)
        case types => Left(new Exception(s"${types.map(_.show).mkString(", ")} cannot be converted to a NodeType"))
      }
    }
  }
}

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
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.literal._
import io.renku.graph.model.entities.{Activity, Entity}
import io.renku.jsonld.{EntityId, EntityType}
import io.renku.knowledgegraph.lineage.model.Node.Location
import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}
import org.apache.jena.util.URIref

import java.time.Instant

object model {

  private[lineage] final case class ExecutionInfo(entityId: EntityId, date: RunDate)

  private[lineage] trait RunDate extends Any with InstantTinyType

  private[lineage] final case class RunDateImpl(value: Instant) extends AnyVal with RunDate
  private[lineage] implicit object RunDate                      extends TinyTypeFactory[RunDate](RunDateImpl)
  private[lineage] type FromAndToNodes = (Set[Node.Location], Set[Node.Location])
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

    implicit val lineageEncoder: Encoder[Lineage] = deriveEncoder
  }

  trait LineageOps {
    self: Lineage =>

    def getNode(location: Location): Option[Node] = nodes.find(_.location == location)
  }

  final case class Edge(source: Node.Location, target: Node.Location)

  object Edge {
    implicit val edgeEncoder: Encoder[Edge] = Encoder.instance { edge =>
      json"""{
        "source": ${edge.source.value},
        "target": ${edge.target.value}
      }"""
    }
  }

  final case class Node(location: Node.Location, label: Node.Label, typ: Node.Type)

  object Node {
    import io.renku.tinytypes.constraints.NonBlank
    import io.renku.tinytypes.{StringTinyType, TinyTypeFactory}

    final class Id private (val value: String) extends AnyVal with StringTinyType
    object Id extends TinyTypeFactory[Id](new Id(_)) with NonBlank[Id] {
      import io.renku.graph.model.views.RdfResource
      import io.renku.tinytypes.Renderer

      implicit val rdfResourceRenderer: Renderer[RdfResource, Id] = value => s"<${URIref.encode(value.show)}>"
    }

    final class Label private (val value: String) extends AnyVal with StringTinyType
    object Label                                  extends TinyTypeFactory[Label](new Label(_)) with NonBlank[Label]

    sealed trait Type extends Any with StringTinyType with Product with Serializable
    object Type extends TinyTypeFactory[Type](TypeInstantiator) {

      case object ProcessRun extends Type { val value: String = "ProcessRun" }
      case object File       extends Type { val value: String = "File" }
      case object Directory  extends Type { val value: String = "Directory" }

      val all: Set[Type] = Set(ProcessRun, File, Directory)

      def fromEntityTypes(types: Set[String]): Either[IllegalArgumentException, Type] =
        types.map(t => EntityType.of(t.show)) match {
          case types if Activity.entityTypes.toList.toSet === types     => Right(ProcessRun)
          case types if Entity.folderEntityTypes.toList.toSet === types => Right(Directory)
          case types if Entity.fileEntityTypes.toList.toSet === types   => Right(File)
          case types =>
            Left(new IllegalArgumentException(s"${types.map(_.show).mkString(", ")} cannot be converted to a NodeType"))
        }
    }

    private object TypeInstantiator extends (String => Type) {
      override def apply(value: String): Type = Type.all.find(_.value == value).getOrElse {
        throw new IllegalArgumentException(s"'$value' unknown Node Type")
      }
    }

    final class Location private (val value: String) extends AnyVal with StringTinyType
    object Location extends TinyTypeFactory[Location](new Location(_)) with NonBlank[Location] {
      def unapply(value: String): Option[Location] = Location.from(value).toOption
    }

    implicit val nodeEncoder: Encoder[Node] = Encoder.instance { node =>
      json"""{
        "id":       ${node.location.value},
        "location": ${node.location.value},
        "label":    ${node.label.value},
        "type":     ${node.typ.show}
      }"""
    }
  }
}

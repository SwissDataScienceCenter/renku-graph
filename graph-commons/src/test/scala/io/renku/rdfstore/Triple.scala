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

package io.renku.rdfstore

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.views.RdfResource
import io.renku.jsonld.{EntityId, Property}
import io.renku.tinytypes.Renderer

sealed trait Triple {
  val subject:   EntityId
  val predicate: Property
  type O
  val `object`: O

  def asSparql: String
}

object Triple {

  final case class Value(subject: EntityId, predicate: Property, `object`: String) extends Triple {
    override type O = String

    override lazy val asSparql: String = show"""<$subject> $predicate ${`object`}"""
  }

  final case class Edge(subject: EntityId, predicate: Property, `object`: EntityId) extends Triple {
    override type O = EntityId

    override lazy val asSparql: String = show"""<$subject> $predicate <${`object`}>"""
  }

  def apply(subject: EntityId, predicate: Property, `object`: String): Triple =
    Value(subject, predicate, s"${`object`}")

  def apply[S](subject: S, predicate: Property, `object`: String)(implicit
      sRenderer:        Renderer[RdfResource, S]
  ): Triple = Value(EntityId.of(sRenderer render subject), predicate, s"${`object`}")

  def edge[S, O](subject: S, predicate: Property, `object`: O)(implicit
      sRenderer:          Renderer[RdfResource, S],
      oRenderer:          Renderer[RdfResource, O]
  ): Triple = Edge(EntityId.of(sRenderer render subject), predicate, EntityId.of(oRenderer render `object`))

  def edge(subject: EntityId, predicate: Property, `object`: EntityId): Triple =
    Edge(subject, predicate, `object`)

  implicit def show: Show[Triple] = Show.show {
    case t: Value => t.show
    case t: Edge  => t.show
  }

  implicit val valueShow: Show[Value] = Show.show(_.asSparql)
  implicit val edgeShow:  Show[Edge]  = Show.show(_.asSparql)
}

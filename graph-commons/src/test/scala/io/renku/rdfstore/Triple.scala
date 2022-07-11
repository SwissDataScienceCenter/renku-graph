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
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.jsonld.{EntityId, EntityIdEncoder, Property}
import io.renku.tinytypes.{StringTinyType, TinyType}

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

    override lazy val asSparql: String = show"""<$subject> <$predicate> ${`object`}"""
  }

  final case class Edge(subject: EntityId, predicate: Property, `object`: EntityId) extends Triple {
    override type O = EntityId

    override lazy val asSparql: String = show"""<$subject> <$predicate> <${`object`}>"""
  }

  def apply(subject: EntityId, predicate: Property, `object`: String): Triple =
    Value(subject, predicate, s"'${sparqlEncode(`object`)}'")

  def apply[O <: StringTinyType](subject: EntityId, predicate: Property, `object`: O)(implicit oShow: Show[O]): Triple =
    apply(subject, predicate, `object`.show)

  def apply[S, O <: TinyType](subject: S, predicate: Property, `object`: O)(implicit
      sEncoder:                        EntityIdEncoder[S],
      oShow:                           Show[O]
  ): Triple = apply(subject, predicate, `object`.show)

  def apply[S](subject: S, predicate: Property, `object`: String)(implicit
      sEncoder:         EntityIdEncoder[S]
  ): Triple = Value(sEncoder(subject), predicate, s"'${sparqlEncode(`object`)}'")

  def edge[S, O](subject: S, predicate: Property, `object`: O)(implicit
      sEncoder:           EntityIdEncoder[S],
      oEncoder:           EntityIdEncoder[O]
  ): Triple = Edge(sEncoder(subject), predicate, oEncoder(`object`))

  def edge[O](subject: EntityId, predicate: Property, `object`: O)(implicit
      oEncoder:        EntityIdEncoder[O]
  ): Triple = Edge(subject, predicate, oEncoder(`object`))

  def edge[S](subject: S, predicate: Property, `object`: EntityId)(implicit
      sEncoder:        EntityIdEncoder[S]
  ): Triple = Edge(sEncoder(subject), predicate, `object`)

  def edge(subject: EntityId, predicate: Property, `object`: EntityId): Triple =
    Edge(subject, predicate, `object`)

  implicit def show: Show[Triple] = Show.show {
    case t: Value => t.show
    case t: Edge  => t.show
  }

  implicit val valueShow: Show[Value] = Show.show(_.asSparql)
  implicit val edgeShow:  Show[Edge]  = Show.show(_.asSparql)
}

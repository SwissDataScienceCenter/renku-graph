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

package io.renku.triplesstore

import cats.Show
import cats.syntax.all._
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.jsonld.{EntityId, EntityIdEncoder, Property}
import io.renku.tinytypes.{StringTinyType, TinyType}

final case class Quad(graphId: EntityId, triple: Triple)

object Quad {

  import Triple._

  def apply(graphId: EntityId, subject: EntityId, predicate: Property, `object`: String): Quad =
    Quad(graphId, Value(subject, predicate, s"'${sparqlEncode(`object`)}'"))

  def apply[O <: StringTinyType](graphId: EntityId, subject: EntityId, predicate: Property, `object`: O)(implicit
      oShow:                              Show[O]
  ): Quad = apply(graphId, subject, predicate, `object`.show)

  def apply[S, O <: TinyType](graphId: EntityId, subject: S, predicate: Property, `object`: O)(implicit
      sEncoder:                        EntityIdEncoder[S],
      oShow:                           Show[O]
  ): Quad = apply(graphId, subject, predicate, `object`.show)

  def apply[S](graphId: EntityId, subject: S, predicate: Property, `object`: String)(implicit
      sEncoder:         EntityIdEncoder[S]
  ): Quad = Quad(graphId, Value(sEncoder(subject), predicate, s"'${sparqlEncode(`object`)}'"))

  def edge[S, O](graphId: EntityId, subject: S, predicate: Property, `object`: O)(implicit
      sEncoder:           EntityIdEncoder[S],
      oEncoder:           EntityIdEncoder[O]
  ): Quad = Quad(graphId, Edge(sEncoder(subject), predicate, oEncoder(`object`)))

  def edge[O](graphId: EntityId, subject: EntityId, predicate: Property, `object`: O)(implicit
      oEncoder:        EntityIdEncoder[O]
  ): Quad = Quad(graphId, Edge(subject, predicate, oEncoder(`object`)))

  def edge[S](graphId: EntityId, subject: S, predicate: Property, `object`: EntityId)(implicit
      sEncoder:        EntityIdEncoder[S]
  ): Quad = Quad(graphId, Edge(sEncoder(subject), predicate, `object`))

  def edge(graphId: EntityId, subject: EntityId, predicate: Property, `object`: EntityId): Quad =
    Quad(graphId, Edge(subject, predicate, `object`))

  implicit def show: Show[Quad] = Show.show { quad =>
    show"""|GRAPH <${quad.graphId.show}> {
           |  ${quad.triple}
           |}""".stripMargin
  }
}

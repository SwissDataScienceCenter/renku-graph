/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore.model

import io.renku.jsonld._

final case class Quad(graphId: EntityId, triple: Triple)

object Quad {

  def apply(graphId: EntityId, subject: EntityId, predicate: Property, obj: TripleObject): Quad =
    Quad(graphId, Triple(subject, predicate, obj))

//  import SparqlEncoder._
//
//  def apply[S, O](graph:  EntityId, subject: S, predicate: Property, obj: O)(implicit
//                                                                             subjectEntityIdEnc: EntityIdEncoder[S],
//                                                                             objEnc:             SparqlEncoder[O]
//  ): Quad = new Quad(graph, subjectEntityIdEnc(subject), predicate, objEnc(obj))
//
//  def apply[S](graph:     EntityId, subject: S, predicate: Property, obj: Property)(implicit
//                                                                                    subjectEntityIdEnc: EntityIdEncoder[S]
//  ): Quad = new Quad(graph, subjectEntityIdEnc(subject), predicate, entityIdEncoder(EntityId.of(obj)))
//
//  implicit lazy val sparqlEncoder: SparqlEncoder[Quad] = SparqlEncoder.instance {
//    case Quad(graph, subject, predicate, obj) =>
//      s"""GRAPH ${graph.toSparql} { ${subject.toSparql} ${predicate.toSparql} $obj }"""
//  }
//
//  implicit def show: Show[Quad] = Show.show(sparqlEncoder)
}

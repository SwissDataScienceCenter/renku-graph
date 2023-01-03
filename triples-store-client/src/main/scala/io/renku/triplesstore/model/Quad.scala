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

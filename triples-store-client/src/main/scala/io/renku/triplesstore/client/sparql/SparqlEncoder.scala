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

package io.renku.triplesstore.client.sparql

import cats.Contravariant
import cats.syntax.all._
import io.renku.jsonld.ontology.xsd
import io.renku.jsonld.{EntityId, Property}
import io.renku.triplesstore.client.model.{Quad, Triple, TripleObject}
import org.apache.jena.util.URIref
import org.eclipse.rdf4j.query.parser.sparql.SPARQLQueries

trait SparqlEncoder[T] extends (T => Fragment)

object SparqlEncoder {

  def apply[V](implicit instance: SparqlEncoder[V]): SparqlEncoder[V] = instance

  def instance[T](f: T => Fragment): SparqlEncoder[T] = (t: T) => f(t)

  private def nonStringLiteralEncoder: SparqlEncoder[String] =
    SparqlEncoder.instance(Fragment(_))

  private def stringLiteralEncoder: SparqlEncoder[String] =
    SparqlEncoder.instance(v => Fragment(s"'${SPARQLQueries.escape(v)}'"))

  private def iriEncoder: SparqlEncoder[String] =
    SparqlEncoder.instance(v => Fragment(s"<${URIref.encode(v)}>"))

  implicit lazy val contravariant: Contravariant[SparqlEncoder] = new Contravariant[SparqlEncoder] {
    def contramap[A, B](fa: SparqlEncoder[A])(f: B => A): SparqlEncoder[B] =
      SparqlEncoder[B](b => fa(f(b)))
  }

  object Instances extends Instances
  trait Instances {

    implicit val entityIdSparqlEncoder: SparqlEncoder[EntityId] = iriEncoder.contramap(_.show)
    implicit val propertySparqlEncoder: SparqlEncoder[Property] = iriEncoder.contramap(_.show)

    private val booleanObjectEncoder: SparqlEncoder[TripleObject.Boolean] =
      nonStringLiteralEncoder.contramap(_.value.toString)
    private val intObjectEncoder: SparqlEncoder[TripleObject.Int] =
      nonStringLiteralEncoder.contramap(_.value.toString)
    private val longObjectEncoder: SparqlEncoder[TripleObject.Long] =
      nonStringLiteralEncoder.contramap(_.value.toString)
    private val floatObjectEncoder: SparqlEncoder[TripleObject.Float] =
      nonStringLiteralEncoder.contramap(_.value.toString)
    private val doubleObjectEncoder: SparqlEncoder[TripleObject.Double] =
      nonStringLiteralEncoder.contramap(_.value.toString)
    private val stringObjectEncoder: SparqlEncoder[TripleObject.String] = stringLiteralEncoder.contramap(_.value)
    private val instantObjectEncoder: SparqlEncoder[TripleObject.Instant] =
      SparqlEncoder.instance(v =>
        Fragment(s"'${SPARQLQueries.escape(v.value.toString)}'^^${propertySparqlEncoder(xsd / "dateTime").sparql}")
      )
    private val localDateObjectEncoder: SparqlEncoder[TripleObject.LocalDate] =
      SparqlEncoder.instance(v =>
        Fragment(s"'${SPARQLQueries.escape(v.value.toString)}'^^${propertySparqlEncoder(xsd / "date").sparql}")
      )
    private val iriObjectEncoder: SparqlEncoder[TripleObject.Iri] = entityIdSparqlEncoder.contramap(_.value)
    implicit def tripleObjectSparqlEncoder[T <: TripleObject]: SparqlEncoder[T] = SparqlEncoder.instance {
      case o: TripleObject.Boolean   => booleanObjectEncoder(o)
      case o: TripleObject.Int       => intObjectEncoder(o)
      case o: TripleObject.Long      => longObjectEncoder(o)
      case o: TripleObject.Float     => floatObjectEncoder(o)
      case o: TripleObject.Double    => doubleObjectEncoder(o)
      case o: TripleObject.String    => stringObjectEncoder(o)
      case o: TripleObject.Instant   => instantObjectEncoder(o)
      case o: TripleObject.LocalDate => localDateObjectEncoder(o)
      case o: TripleObject.Iri       => iriObjectEncoder(o)
    }

    implicit val tripleSparqlEncoder: SparqlEncoder[Triple] = SparqlEncoder.instance {
      case Triple(subject, predicate, obj) =>
        Fragment(
          s"${entityIdSparqlEncoder(subject).sparql} ${propertySparqlEncoder(predicate).sparql} ${tripleObjectSparqlEncoder(obj).sparql}."
        )
    }

    implicit val quadSparqlEncoder: SparqlEncoder[Quad] = SparqlEncoder.instance { case Quad(graphId, triple) =>
      Fragment(
        s"GRAPH ${entityIdSparqlEncoder(graphId).sparql} { ${tripleSparqlEncoder(triple).sparql} }"
      )
    }
  }
}

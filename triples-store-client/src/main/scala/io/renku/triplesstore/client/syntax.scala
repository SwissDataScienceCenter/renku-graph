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

package io.renku.triplesstore.client

import io.renku.jsonld.{EntityId, Property}
import io.renku.triplesstore.client.model._
import io.renku.triplesstore.client.sparql.{Fragment, LuceneQuery, SparqlEncoder, VarName}

import java.time.{Instant, LocalDate}

object syntax extends TripleObjectEncoder.Instances with SparqlEncoder.Instances {

  final implicit class TSClientObjectOps[T](private val obj: T) extends AnyVal {

    def asQuads(implicit enc: QuadsEncoder[T]): Set[Quad] = enc(obj)

    def asTripleObject(implicit enc: TripleObjectEncoder[T]): TripleObject = enc(obj)

    def asSparql(implicit enc: SparqlEncoder[T]): Fragment = enc(obj)
  }

  final implicit class FragmentStringContext(private val sc: StringContext) {

    def sparql(args: Any*): Fragment = fr(args: _*)

    def fr(args: Any*): Fragment = {
      val values = args.map(makeValue)
      Fragment(sc.s(values: _*))
    }

    private def makeValue(arg: Any): String = arg match {
      case q:        LuceneQuery  => q.asSparql.sparql
      case s:        String       => s.asTripleObject.asSparql.sparql
      case s:        Char         => s.toString.asTripleObject.asSparql.sparql
      case n:        Float        => n.asTripleObject.asSparql.sparql
      case n:        Long         => n.asTripleObject.asSparql.sparql
      case n:        Double       => n.asTripleObject.asSparql.sparql
      case b:        Boolean      => b.asTripleObject.asSparql.sparql
      case dt:       Instant      => dt.asTripleObject.asSparql.sparql
      case ld:       LocalDate    => ld.asTripleObject.asSparql.sparql
      case entityId: EntityId     => entityId.asSparql.sparql
      case tr:       Triple       => tr.asSparql.sparql
      case q:        Quad         => q.asSparql.sparql
      case to:       TripleObject => to.asSparql.sparql
      case p:        Property     => p.asSparql.sparql
      case f:        Fragment     => f.sparql
      case v:        VarName      => v.name
      case seq:      Iterable[Any] =>
        seq.map(makeValue).map(s => s"($s)").mkString(" ")
      case opt: Option[Any] =>
        opt.map(makeValue).getOrElse("")
      case _ => sys.error(s"Unsupported value type '${arg.getClass}: $arg'")
    }
  }
}

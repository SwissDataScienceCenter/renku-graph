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

import io.renku.jsonld.{EntityId, Property}
import io.renku.triplesstore.client.model.{Quad, Triple, TripleObject}
import io.renku.triplesstore.client.syntax._

import java.time.{Instant, LocalDate}

class StringInterpolator(private val sc: StringContext) {

  import StringInterpolator.ClauseDetector

  def sparql(args: Any*): Fragment = fr(args: _*)

  def fr(args: Any*): Fragment = {
    val values = args.zipWithIndex.map(makeValue)
    Fragment(sc.s(values: _*))
  }

  private lazy val makeValue: ((Any, Int)) => String = {
    case (a: LuceneQuery, _)      => a.asSparql.sparql
    case (a: String, _)           => a.asTripleObject.asSparql.sparql
    case (a: Char, _)             => a.toString.asTripleObject.asSparql.sparql
    case (a: Float, _)            => a.asTripleObject.asSparql.sparql
    case (a: Long, _)             => a.asTripleObject.asSparql.sparql
    case (a: Double, _)           => a.asTripleObject.asSparql.sparql
    case (a: Boolean, _)          => a.asTripleObject.asSparql.sparql
    case (a: Instant, _)          => a.asTripleObject.asSparql.sparql
    case (a: LocalDate, _)        => a.asTripleObject.asSparql.sparql
    case (a: EntityId, _)         => a.asSparql.sparql
    case (a: Triple, _)           => a.asSparql.sparql
    case (a: Quad, _)             => a.asSparql.sparql
    case (a: TripleObject, _)     => a.asSparql.sparql
    case (a: Property, _)         => a.asSparql.sparql
    case (a: Fragment, _)         => a.sparql
    case (a: VarName, _)          => a.name
    case (it: Iterable[Any], idx) => resolveIterable(it, idx)
    case (opt: Option[Any], idx)  => opt.map(makeValue(_, idx)).getOrElse("")
    case (arg, _)                 => sys.error(s"Unsupported value type '${arg.getClass}: $arg'")
  }

  private def resolveIterable(it: Iterable[Any], idx: Int) = {
    import ClauseDetector.ClauseType
    ClauseDetector
      .detectClauseContext(sc.parts(idx))
      .map {
        case ClauseType.VALUES_BRACKETED     => it.map(makeValue(_, idx)).map(s => s"($s)").mkString(" ")
        case ClauseType.VALUES_NOT_BRACKETED => it.map(makeValue(_, idx)).mkString(" ")
        case ClauseType.IN                   => it.map(makeValue(_, idx)).mkString(", ")
      }
      .getOrElse(sys.error("Iterable cannot be resolved in this context"))
  }
}

private object StringInterpolator {

  private object ClauseDetector {
    private val valuesBracketedClause    = ".*VALUES\\s*\\(\\s*\\?\\w+\\s*\\)\\s*\\{\\s*$".r
    private val valuesNotBracketedClause = ".*VALUES\\s*\\?\\w+\\s*\\{\\s*$".r
    private val inClause                 = ".*IN\\s*\\(\\s*$".r

    sealed trait ClauseType
    object ClauseType {
      case object VALUES_BRACKETED     extends ClauseType
      case object VALUES_NOT_BRACKETED extends ClauseType
      case object IN                   extends ClauseType
    }

    def detectClauseContext(snippet: String): Option[ClauseType] = {
      val validated = snippet.filter(c => c >= ' ' && c != '|').toUpperCase
      if (valuesBracketedClause matches validated) Some(ClauseType.VALUES_BRACKETED)
      else if (valuesNotBracketedClause matches validated) Some(ClauseType.VALUES_NOT_BRACKETED)
      else if (inClause matches validated) Some(ClauseType.IN)
      else Option.empty
    }
  }
}

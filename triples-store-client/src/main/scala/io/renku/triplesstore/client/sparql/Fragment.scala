/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import cats.{Monoid, Show}
import io.renku.triplesstore.client.http.{SparqlQuery, SparqlUpdate}

final case class Fragment(sparql: String) extends SparqlUpdate with SparqlQuery {
  def isEmpty:  Boolean = sparql.isBlank
  def nonEmpty: Boolean = !isEmpty

  def ++(next: Fragment): Fragment =
    (sparql, next.sparql) match {
      case ("", r) => Fragment(r)
      case (l, "") => Fragment(l)
      case (l, r)  => Fragment(s"$l $r")
    }

  def stripMargin: Fragment =
    Fragment(sparql.stripMargin)

  override def render = sparql
}

object Fragment {

  val empty: Fragment = Fragment("")

  implicit val show: Show[Fragment] = Show.show(_.sparql)
  implicit val monoid: Monoid[Fragment] =
    Monoid.instance(empty, _ ++ _)
}

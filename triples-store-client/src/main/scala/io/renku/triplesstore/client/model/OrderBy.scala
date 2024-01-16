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

package io.renku.triplesstore.client.model

import cats.Semigroup
import cats.data.NonEmptyList
import io.renku.triplesstore.client.model.OrderBy.Sort
import io.renku.triplesstore.client.sparql.{Fragment, SparqlEncoder}

final case class OrderBy(sorting: NonEmptyList[Sort]) {
  def ++(other: OrderBy): OrderBy =
    OrderBy(sorting.concatNel(other.sorting))
}

object OrderBy {

  sealed trait Direction extends Product {
    final val name: String = productPrefix.toUpperCase
    def apply(property: Property): Sort = Sort(property, this)
  }
  object Direction {
    case object Asc  extends Direction
    case object Desc extends Direction

    val all: NonEmptyList[Direction] = NonEmptyList.of(Asc, Desc)
  }

  final class Property(val name: String) extends AnyVal
  object Property {
    def apply(name: String): Property = new Property(name)
  }

  final case class Sort(property: Property, direction: Direction)

  def apply(property: Property, direction: Direction): OrderBy =
    OrderBy(NonEmptyList.of(Sort(property, direction)))

  def apply(p: (Property, Direction), more: (Property, Direction)*): OrderBy =
    OrderBy(NonEmptyList(p, more.toList).map((Sort.apply _).tupled))

  def apply(p: Sort, more: Sort*): OrderBy =
    OrderBy(NonEmptyList(p, more.toList))

  implicit val orderBySemigroup: Semigroup[OrderBy] =
    Semigroup.instance(_ ++ _)

  implicit val sparqlFragmentEncoder: SparqlEncoder[OrderBy] =
    SparqlEncoder.instance { orderBy =>
      val props = orderBy.sorting.map(s => s"${s.direction.name.toUpperCase}(${s.property.name})").toList.mkString(" ")
      Fragment(s"ORDER BY $props")
    }
}

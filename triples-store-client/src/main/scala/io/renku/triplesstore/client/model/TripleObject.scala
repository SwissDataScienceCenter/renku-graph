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

package io.renku.triplesstore.client.model

import cats.Show
import cats.syntax.all._
import io.renku.jsonld.EntityId

sealed trait TripleObject extends Any with Product with Serializable {
  type T
  def value: T
}

object TripleObject {

  final case class Boolean(value: scala.Boolean) extends AnyVal with TripleObject {
    override type T = scala.Boolean
  }

  final case class Int(value: scala.Int) extends AnyVal with TripleObject {
    override type T = scala.Int
  }

  final case class Long(value: scala.Long) extends AnyVal with TripleObject {
    override type T = scala.Long
  }

  final case class Float(value: scala.Float) extends AnyVal with TripleObject {
    override type T = scala.Float
  }

  final case class Double(value: scala.Double) extends AnyVal with TripleObject {
    override type T = scala.Double
  }

  final case class String(value: Predef.String) extends AnyVal with TripleObject {
    override type T = Predef.String
  }

  final case class Instant(value: java.time.Instant) extends AnyVal with TripleObject {
    override type T = java.time.Instant
  }

  final case class Iri(value: EntityId) extends TripleObject {
    override type T = EntityId
  }

  implicit def show[T <: TripleObject]: Show[T] = Show.show {
    case v: TripleObject.Boolean => v.value.show
    case v: TripleObject.Int     => v.value.show
    case v: TripleObject.Long    => v.value.show
    case v: TripleObject.Float   => v.value.show
    case v: TripleObject.Double  => v.value.show
    case v: TripleObject.String  => v.value.show
    case v: TripleObject.Instant => v.value.toString
    case v: TripleObject.Iri     => v.value.show
  }
}

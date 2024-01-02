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

import cats.Contravariant
import cats.syntax.all._
import io.renku.jsonld.{EntityId, Property}

import java.time.{Instant => JInstant, LocalDate => JLocalDate}

trait TripleObjectEncoder[O] extends (O => TripleObject)

object TripleObjectEncoder {

  def apply[V](implicit instance: TripleObjectEncoder[V]): TripleObjectEncoder[V] = instance

  def instance[T](f: T => TripleObject): TripleObjectEncoder[T] = (t: T) => f(t)

  implicit lazy val objectContravariant: Contravariant[TripleObjectEncoder] = new Contravariant[TripleObjectEncoder] {
    def contramap[A, B](fa: TripleObjectEncoder[A])(f: B => A): TripleObjectEncoder[B] =
      TripleObjectEncoder[B](b => fa(f(b)))
  }

  object Instances extends Instances
  trait Instances {

    implicit val booleanObjEncoder: TripleObjectEncoder[scala.Boolean] =
      TripleObjectEncoder.instance(TripleObject.Boolean)
    implicit val charObjEncoder:   TripleObjectEncoder[scala.Char]   = TripleObjectEncoder.instance(TripleObject.Char)
    implicit val intObjEncoder:    TripleObjectEncoder[scala.Int]    = TripleObjectEncoder.instance(TripleObject.Int)
    implicit val longObjEncoder:   TripleObjectEncoder[scala.Long]   = TripleObjectEncoder.instance(TripleObject.Long)
    implicit val floatObjEncoder:  TripleObjectEncoder[scala.Float]  = TripleObjectEncoder.instance(TripleObject.Float)
    implicit val doubleObjEncoder: TripleObjectEncoder[scala.Double] = TripleObjectEncoder.instance(TripleObject.Double)
    implicit val stringObjEncoder: TripleObjectEncoder[Predef.String] =
      TripleObjectEncoder.instance(TripleObject.String)
    implicit val instantObjEncoder: TripleObjectEncoder[JInstant] = TripleObjectEncoder.instance(TripleObject.Instant)
    implicit val localDateObjEncoder: TripleObjectEncoder[JLocalDate] =
      TripleObjectEncoder.instance(TripleObject.LocalDate)
    implicit val entityIdObjEncoder: TripleObjectEncoder[EntityId] = TripleObjectEncoder.instance(TripleObject.Iri)
    implicit val propertyObjEncoder: TripleObjectEncoder[Property] = entityIdObjEncoder.contramap(p => EntityId.of(p))
  }
}

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

package io.renku.graph.model.entities

import cats.Contravariant
import cats.syntax.all._
import io.renku.graph.model.GraphClass
import io.renku.jsonld.JsonLDEncoder

trait EntityFunctions[A] {

  val findAllPersons: A => Set[Person]

  val encoder: GraphClass => JsonLDEncoder[A]
}

object EntityFunctions {

  def apply[A](implicit ev: EntityFunctions[A]): EntityFunctions[A] = ev

  implicit val contravariant: Contravariant[EntityFunctions] =
    new Contravariant[EntityFunctions] {
      override def contramap[A, B](fa: EntityFunctions[A])(f: B => A): EntityFunctions[B] =
        new EntityFunctions[B] {

          override val findAllPersons: B => Set[Person] = { b =>
            fa.findAllPersons(f(b))
          }

          override val encoder: GraphClass => JsonLDEncoder[B] = { gc =>
            fa.encoder(gc).contramap(f)
          }
        }
    }
}

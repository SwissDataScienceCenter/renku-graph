/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package io.renku.jsonld

/**
  * A type class that provides a conversion from a value of type `A` to a [[JsonLD]] value.
  */
trait JsonLDEncoder[A] extends Serializable {

  def apply(a: A): JsonLD
}

object JsonLDEncoder {

  final def instance[A](f: A => JsonLD): JsonLDEncoder[A] = new JsonLDEncoder[A] {
    final def apply(a: A): JsonLD = f(a)
  }

  final def entityId[A](f: A => EntityId): JsonLDEncoder[A] = new JsonLDEncoder[A] {
    final def apply(a: A): JsonLD = JsonLD.fromEntityId(f(a))
  }

  final implicit val encodeString: JsonLDEncoder[String] = new JsonLDEncoder[String] {
    final def apply(a: String): JsonLD = JsonLD.fromString(a)
  }

  final implicit val encodeInt: JsonLDEncoder[Int] = new JsonLDEncoder[Int] {
    final def apply(a: Int): JsonLD = JsonLD.fromInt(a)
  }

  final implicit val encodeLong: JsonLDEncoder[Long] = new JsonLDEncoder[Long] {
    final def apply(a: Long): JsonLD = JsonLD.fromLong(a)
  }
}

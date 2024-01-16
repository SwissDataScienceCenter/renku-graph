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

package io.renku.triplesstore.client.http

import cats.syntax.all._
import io.circe.{Decoder, HCursor}

trait RowDecoder[A] extends Decoder[A]

object RowDecoder {

  def apply[A](implicit d: RowDecoder[A]): RowDecoder[A] = d

  def fromDecoder[A](d: Decoder[A]): RowDecoder[A] =
    (c: HCursor) => d.apply(c)

  private def prop[A: Decoder](name: String): Decoder[A] =
    Decoder.instance { cursor =>
      cursor.downField(name).downField("value").as[A]
    }

  def forProduct1[T, A0](name: String)(f: A0 => T)(implicit d: Decoder[A0]): RowDecoder[T] =
    fromDecoder(prop(name).map(f))

  def forProduct2[T, A0: Decoder, A1: Decoder](name0: String, name1: String)(f: (A0, A1) => T): RowDecoder[T] =
    fromDecoder((prop[A0](name0), prop[A1](name1)).mapN(f))

  def forProduct3[T, A0: Decoder, A1: Decoder, A2: Decoder](name0: String, name1: String, name2: String)(
      f: (A0, A1, A2) => T
  ): RowDecoder[T] =
    fromDecoder((prop[A0](name0), prop[A1](name1), prop[A2](name2)).mapN(f))

  def forProduct4[T, A0: Decoder, A1: Decoder, A2: Decoder, A3: Decoder](
      name0: String,
      name1: String,
      name2: String,
      name3: String
  )(
      f: (A0, A1, A2, A3) => T
  ): RowDecoder[T] =
    fromDecoder((prop[A0](name0), prop[A1](name1), prop[A2](name2), prop[A3](name3)).mapN(f))

}

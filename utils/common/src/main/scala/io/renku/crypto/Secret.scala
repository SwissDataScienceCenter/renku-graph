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

package io.renku.crypto

import scodec.bits.ByteVector

import java.nio.charset.CharacterCodingException

final class Secret private (val value: ByteVector) extends AnyVal {
  override def toString: String = "<sensitive>"

  def decodeAscii: Either[CharacterCodingException, String] = value.decodeAscii
}

object Secret {

  def apply(value: ByteVector): Either[String, Secret] = {
    val expectedLengths = List(16L, 24L, 32L)
    if (expectedLengths contains value.length) Right(new Secret(value))
    else Left(s"Expected ${expectedLengths.mkString(" or ")} bytes, but got ${value.length}")
  }

  def unsafe(value: ByteVector): Secret = apply(value).fold(sys.error, identity)

  private def fromBase64(b64: String): Either[String, Secret] =
    ByteVector.fromBase64Descriptive(b64).flatMap(apply)

  def unsafeFromBase64(b64: String): Secret =
    fromBase64(b64).fold(sys.error, identity)

}

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

package io.renku.knowledgegraph.multipart

import io.circe.Json
import io.renku.tinytypes.TinyType
import org.http4s.Header.ToRaw._
import org.http4s.MediaType
import org.http4s.headers.`Content-Type`
import org.http4s.multipart.Part

trait PartEncoder[F[_], A] {
  def apply(partName: String, a: A): Part[F]
}

object PartEncoder {

  def instance[F[_], A](f: (String, A) => Part[F]): PartEncoder[F, A] =
    (partName: String, a: A) => f(partName, a)

  implicit def fromPartValueEncoder[F[_], A](implicit pvEnc: PartValueEncoder[A]): PartEncoder[F, A] =
    instance[F, A]((partName: String, a: A) => Part.formData[F](partName, pvEnc(a)))

  implicit def fromPartJsonValueEncoder[F[_]]: PartEncoder[F, Json] =
    instance[F, Json]((partName: String, a: Json) =>
      Part.formData[F](partName, a.noSpaces, `Content-Type`(MediaType.application.json))
    )
}

trait PartValueEncoder[A] {
  def apply(a: A): String
}

object PartValueEncoder {

  def instance[A](f: A => String): PartValueEncoder[A] = (a: A) => f(a)

  implicit def stringEnc: PartValueEncoder[String] =
    instance(identity)

  implicit def stringTinyTypePartEnc[TT <: TinyType { type V = String }]: PartValueEncoder[TT] =
    instance(_.value)

  implicit def intTinyTypePartEnc[TT <: TinyType { type V = Int }]: PartValueEncoder[TT] =
    instance(_.value.toString)
}

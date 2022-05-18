/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.tinytypes.constraints

import UrlEncoder._
import io.renku.tinytypes._

trait RelativePath[TT <: TinyType { type V = String }] extends Constraints[TT] with NonBlank[TT] {
  self: TinyTypeFactory[TT] =>

  addConstraint(
    check = value => !value.startsWith("/") && !value.endsWith("/") && !value.matches("^\\w+://.*"),
    message = value => s"'$value' is not a valid $typeName"
  )
}

trait RelativePathOps[T <: RelativePathTinyType] {
  self: TinyTypeFactory[T] with RelativePath[T] =>

  implicit class RelativePathOps(url: T) {

    def /(value: String): T =
      apply(s"$url/${urlEncode(value)}")

    def /[TT <: TinyType](value: TT)(implicit converter: TT => List[PathSegment]): T =
      apply(s"$url/${converter(value).mkString("/")}")
  }
}

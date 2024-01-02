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

package io.renku.tinytypes.syntax

import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}
import scala.concurrent.duration.FiniteDuration

trait InstantTinyTypeSyntax {

  final implicit class InstantTinyTypeOps[A <: InstantTinyType](self: A) {
    def +(duration: FiniteDuration)(implicit tf: TinyTypeFactory[A]): A =
      tf.apply(self.value.plusMillis(duration.toMillis))

    def -(duration: FiniteDuration)(implicit tf: TinyTypeFactory[A]): A =
      tf.apply(self.value.minusMillis(duration.toMillis))
  }
}

object InstantTinyTypeSyntax extends InstantTinyTypeSyntax

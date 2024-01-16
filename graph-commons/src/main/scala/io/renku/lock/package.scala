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

package io.renku

import cats.data.Kleisli
import cats.effect._

package object lock {

  type Lock[F[_], A] = Kleisli[Resource[F, *], A, Unit]

  object syntax {
    final implicit class LockSyntax[F[_], A](self: Lock[F, A]) {
      def surround[B](fa: A => F[B])(implicit F: MonadCancelThrow[F]): A => F[B] =
        a => self(a).surround(fa(a))

      def surround[B](k: Kleisli[F, A, B])(implicit F: MonadCancelThrow[F]): Kleisli[F, A, B] =
        Kleisli(surround(k.run))
    }
  }
}

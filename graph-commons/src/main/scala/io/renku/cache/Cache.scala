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

package io.renku.cache

trait Cache[F[_], A, B] {

  def withCache(f: A => F[Option[B]]): A => F[Option[B]]
}

object Cache {
  def apply[F[_], A, B](f: (A => F[Option[B]]) => A => F[Option[B]]): Cache[F, A, B] =
    (n: A => F[Option[B]]) => f(n)

  def noop[F[_], A, B]: Cache[F, A, B] =
    (f: A => F[Option[B]]) => f
}

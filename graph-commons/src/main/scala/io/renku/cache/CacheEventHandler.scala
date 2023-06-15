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

package io.renku.cache

import cats.{Applicative, ApplicativeError, Parallel}
import cats.syntax.all._

trait CacheEventHandler[F[_]] {

  def apply(event: CacheEvent): F[Unit]

}

object CacheEventHandler {
  def apply[F[_]](f: CacheEvent => F[Unit]): CacheEventHandler[F] =
    (event: CacheEvent) => f(event)

  def none[F[_]: Applicative]: CacheEventHandler[F] =
    apply(_ => Applicative[F].unit)

  def ignoreErrors[F[_]](h: CacheEventHandler[F])(implicit F: ApplicativeError[F, Throwable]): CacheEventHandler[F] =
    CacheEventHandler(ev => h(ev).attempt.void)

  def parCombine[F[_]: Parallel: Applicative](handlers: List[CacheEventHandler[F]]): CacheEventHandler[F] =
    if (handlers.isEmpty) none[F]
    else apply(ev => handlers.parTraverse_(h => h(ev)))

  def combine[F[_]: Applicative](handlers: List[CacheEventHandler[F]]): CacheEventHandler[F] =
    if (handlers.isEmpty) none[F]
    else apply(ev => handlers.traverse_(h => h(ev)))
}

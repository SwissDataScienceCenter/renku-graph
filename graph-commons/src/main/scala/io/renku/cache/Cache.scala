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

import cats.effect._
import cats.syntax.all._
import fs2.Stream
import io.renku.cache.CacheConfig.ClearConfig

trait Cache[F[_], A, B] {

  def withCache(f: A => F[Option[B]]): A => F[Option[B]]
}

object Cache {
  def apply[F[_], A, B](f: (A => F[Option[B]]) => A => F[Option[B]]): Cache[F, A, B] =
    (n: A => F[Option[B]]) => f(n)

  def noop[F[_], A, B]: Cache[F, A, B] =
    (f: A => F[Option[B]]) => f

  def memoryAsync[F[_]: Async, A, B](cacheConfig: CacheConfig): Resource[F, Cache[F, A, B]] =
    if (cacheConfig.isDisabled) Resource.pure(noop[F, A, B])
    else {
      val refState =
        Ref.of[F, CacheState[A, B]](CacheState.create(cacheConfig))

      Resource.eval(refState).flatMap { state =>
        val cache = baseCache[F, A, B](state)
        val clear = cacheClearing(cacheConfig, state)

        Async[F]
          .background(fs2.Stream.repeatEval(clear).compile.drain)
          .as(cache)
      }
    }

  def memoryAsyncF[F[_]: Async, A, B](cacheConfig: CacheConfig): F[Cache[F, A, B]] =
    if (cacheConfig.isDisabled) noop[F, A, B].pure[F]
    else {
      val refState =
        Ref.of[F, CacheState[A, B]](CacheState.create(cacheConfig))

      refState.flatMap { state =>
        val cache = baseCache[F, A, B](state)
        val clear = cacheClearing(cacheConfig, state)
        Spawn[F].start(clear).as(cache)
      }
    }

  private[cache] def cacheClearing[F[_]: Sync: Temporal, A, B](
      cacheConfig: CacheConfig,
      state:       Ref[F, CacheState[A, B]]
  ) =
    cacheConfig.clearConfig match {
      case ClearConfig.Periodic(_, interval) =>
        Stream
          .awakeEvery(interval)
          .evalMap(_ => state.update(_.shrink))
          .compile
          .drain
    }

  private[cache] def baseCache[F[_]: Sync, A, B](
      state: Ref[F, CacheState[A, B]]
  ): Cache[F, A, B] =
    Cache(f =>
      a =>
        Clock[F].realTime.flatMap(t => state.modify(_.get(a, t))).flatMap {
          case CacheResult.Hit(_, b) => b.pure[F]
          case _ =>
            (Key(a), f(a)).flatMapN { (k, r) =>
              state.update(_.put(k, r)).as(r)
            }
        }
    )
}

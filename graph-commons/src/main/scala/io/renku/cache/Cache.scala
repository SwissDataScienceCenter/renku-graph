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
import fs2.concurrent

trait Cache[F[_], A, B] {

  def withCache(f: A => F[Option[B]]): A => F[Option[B]]
}

object Cache {
  def apply[F[_], A, B](f: (A => F[Option[B]]) => A => F[Option[B]]): Cache[F, A, B] =
    (n: A => F[Option[B]]) => f(n)

  def noop[F[_], A, B]: Cache[F, A, B] =
    (f: A => F[Option[B]]) => f

  def memory[F[_]: Sync, A, B](size: Int, evictStrategy: EvictStrategy, ignoreEmptyValues: Boolean): F[Cache[F, A, B]] =
    if (size <= 0) noop[F, A, B].pure[F]
    else
      Ref.of[F, CacheState[A, B]](CacheState.create(evictStrategy, ignoreEmptyValues)).map { state =>
        Cache(f =>
          a =>
            Clock[F].realTime.flatMap(t => state.modify(_.get(a, t))).flatMap {
              case CacheResult.Hit(b) => b.pure[F]
              case CacheResult.Miss =>
                (f(a), Key(a)).flatMapN { (r, k) =>
                  state.update(_.shrinkTo(size - 1).put(k, r)).as(r)
                }
            }
        )
      }

  def memoryAsync[F[_]: Async, A, B](
      size:              Int,
      evictStrategy:     EvictStrategy,
      ignoreEmptyValues: Boolean
  ): Resource[F, Cache[F, A, B]] = {
    val refState   = Ref.of[F, CacheState[A, B]](CacheState.create(evictStrategy, ignoreEmptyValues))
    val refRefresh = concurrent.SignallingRef.of[F, Long](0L)

    (Resource.eval(refState), Resource.eval(refRefresh)).flatMapN { (state, refresh) =>
      val clear =
        refresh.discrete
          .filter(_ > size.toLong)
          .evalMap(_ => state.update(_.shrinkTo(size)) *> refresh.set(size))
          .compile
          .drain

      val cache: Cache[F, A, B] =
        Cache(f =>
          a =>
            Clock[F].realTime.flatMap(t => state.modify(_.get(a, t))).flatMap {
              case CacheResult.Hit(b) if !evictStrategy.evict(key, b) =>
                b.pure[F]
              case CacheResult.Miss =>
                (f(a), Key(a)).flatMapN { (r, k) =>
                  (refresh.update(_ + 1) *> state.update(_.put(k, r))).as(r)
                }
            }
        )

      Async[F]
        .background(fs2.Stream.repeatEval(clear).compile.drain)
        .as(cache)
    }
  }
}

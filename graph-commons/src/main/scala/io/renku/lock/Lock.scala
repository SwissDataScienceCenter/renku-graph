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

package io.renku.lock

import cats.Functor
import cats.data.Kleisli
import cats.effect._
import cats.effect.std.Mutex
import cats.syntax.all._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

object Lock {

  def none[F[_], A]: Lock[F, A] =
    Kleisli(_ => Resource.unit)

  def global[F[_]: Async, A]: F[Lock[F, A]] =
    Mutex[F].map(mutex => Kleisli(_ => mutex.lock))

  def memory[F[_]: Async, A]: F[Lock[F, A]] =
    memory0[F, A].map(_._2)

  private[lock] def memory0[F[_]: Async, A]: F[(Ref[F, Map[A, Mutex[F]]], Lock[F, A])] =
    Ref.of[F, Map[A, Mutex[F]]](Map.empty[A, Mutex[F]]).map { data =>
      def set(k: A) =
        Resource.eval(Mutex[F]).flatMap { newMutex =>
          val next =
            Resource.eval(
              data.updateAndGet(m =>
                m.updatedWith(k) {
                  case r @ Some(_) => r
                  case None        => Some(newMutex)
                }
              )
            )

          next.flatMap(_(k).lock.onFinalize(data.update(_.removed(k))))
        }

      (data, Kleisli(set))
    }

  def create[F[_]: Temporal, A](acquire: Kleisli[F, A, Boolean], interval: FiniteDuration)(
      release: Kleisli[F, A, Unit]
  ): Lock[F, A] = {
    val acq: Kleisli[F, A, Unit] = Kleisli { key =>
      (Stream.eval(acquire.run(key)) ++ Stream.sleep(interval).drain).repeat
        .find(_ == true)
        .compile
        .drain
    }

    from(acq)(release)
  }

  def from[F[_]: Functor, A](acquire: Kleisli[F, A, Unit])(release: Kleisli[F, A, Unit]): Lock[F, A] =
    Kleisli(key => Resource.make(acquire.run(key))(_ => release.run(key)))
}

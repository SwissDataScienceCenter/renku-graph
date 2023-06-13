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

import cats.Monad
import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

final case class Key[A](value: A, createdAt: Int, accessedAt: Int) {

  def withAccessedAt(time: FiniteDuration): Key[A] =
    new Key(value, createdAt, time.toSeconds.toInt)

  override def toString: String = s"Key($value, createdAt=$createdAt, accessedAt=$accessedAt)"
}

private object Key {
  def apply[F[_]: Clock: Monad, A](value: A): F[Key[A]] =
    Clock[F].realTime.map(_.toSeconds.toInt).map(time => new Key(value, time, time))

  object Order {
    def leastRecentlyUsed[A]: Ordering[Key[A]] = Ordering.by(k => (k.accessedAt.toLong << 32) | k.value.hashCode().abs)

    def oldest[A]: Ordering[Key[A]] = Ordering.by(k => (k.createdAt.toLong << 32) | k.value.hashCode().abs)
  }
}

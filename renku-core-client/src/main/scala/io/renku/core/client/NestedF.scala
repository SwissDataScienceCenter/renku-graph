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

package io.renku.core.client

import cats.Monad
import cats.data.Nested
import cats.syntax.all._

private object NestedF {
  def apply[F[_]: Monad]: NestedF[F] = new NestedF[F]
}

private class NestedF[F[_]: Monad] {

  type NestedF[A] = Nested[F, Result, A]

  implicit lazy val nestedResultMonad: Monad[NestedF] = new Monad[NestedF] {

    override def pure[A](a: A): NestedF[A] =
      Nested {
        Result.success(a).pure[F]
      }

    override def flatMap[A, B](fa: NestedF[A])(f: A => NestedF[B]): NestedF[B] =
      Nested {
        fa.value >>= {
          case Result.Success(a) => f(a).value
          case r: Result.Failure => r.pure[F].widen
        }
      }

    override def map[A, B](fa: NestedF[A])(f: A => B): NestedF[B] =
      Nested {
        fa.value.map(_.map(f))
      }

    override def tailRecM[A, B](a: A)(f: A => NestedF[Either[A, B]]): NestedF[B] = Nested {
      f(a).value >>= {
        case Result.Success(Right(vb)) => Result.success(vb).pure[F]
        case Result.Success(Left(va))  => tailRecM(va)(f).value
        case f: Result.Failure => f.pure[F].widen
      }
    }
  }

  implicit class NestedFOps[A](nestedF: NestedF[A]) {

    def flatMapF[B](f: A => F[Result[B]]): NestedF[B] =
      nestedF.flatMap(a => Nested(f(a)))

    def subflatMap[B](f: A => Result[B]): NestedF[B] =
      nestedF.flatMap(a => Nested(f(a).pure[F]))
  }
}

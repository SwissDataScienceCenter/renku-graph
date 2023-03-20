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

package io.renku.microservices

import cats.data.{Validated, ValidatedNel}
import cats.effect._
import cats.syntax.all._
import org.scalatest.Assertions

trait CallCounter {

  def name: String

  val counter = Ref.unsafe[IO, Either[Throwable, Int]](Right(0))

  def getCount: IO[Either[Throwable, Int]] = counter.get

  def failWith(ex: Throwable): IO[Unit] =
    counter.set(Left(ex))
}

object CallCounter extends Assertions {
  def assertNotCalled(counters: List[CallCounter]) =
    verify(counters,
           (c, n) =>
             if (n == 0) Validated.validNel(n)
             else Validated.invalidNel(s"${c.name} was called $n times")
    )

  def assertCalled(counters: List[CallCounter]) =
    verify(counters,
           (c, n) =>
             if (n >= 1) Validated.validNel(n)
             else Validated.invalidNel(s"${c.name} not called")
    )

  private def verify(counters: List[CallCounter], check: (CallCounter, Int) => ValidatedNel[String, Int]) =
    counters
      .traverse(c =>
        c.getCount.map {
          case Left(ex) =>
            Validated.invalidNel(s"${c.name} failed with: ${ex.getClass}: ${ex.getMessage}")
          case Right(n) =>
            check(c, n)
        }
      )
      .map(_.combineAll)
      .map(_.toEither.leftMap(err => new Exception(err.toList.mkString("; "))))
      .rethrow

}

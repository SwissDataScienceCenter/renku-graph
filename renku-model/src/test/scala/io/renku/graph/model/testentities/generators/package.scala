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

package io.renku.graph.model.testentities

import cats.Monad
import org.scalacheck.Gen

package object generators {
  implicit val genMonad: Monad[Gen] = new Monad[Gen] {
    override def pure[A](x:        A): Gen[A] = Gen.const(x)
    override def flatMap[A, B](fa: Gen[A])(f: A => Gen[B]):            Gen[B] = fa.flatMap(f)
    override def tailRecM[A, B](a: A)(f:      A => Gen[Either[A, B]]): Gen[B] = Gen.tailRecM(a)(f)
  }
}

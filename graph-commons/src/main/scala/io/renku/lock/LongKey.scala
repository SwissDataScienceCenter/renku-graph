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

import io.renku.graph.model.entities.Project
import io.renku.graph.model.projects

import scala.util.hashing.MurmurHash3

trait LongKey[A] { self =>
  def asLong(a: A): Long

  def contramap[B](f: B => A): LongKey[B] =
    LongKey.create(b => self.asLong(f(b)))
}

object LongKey {
  def apply[A](implicit lk: LongKey[A]): LongKey[A] = lk

  def create[A](f: A => Long): LongKey[A] =
    (a: A) => f(a)

  implicit val forLong: LongKey[Long] =
    create(identity)

  implicit val forInt: LongKey[Int] =
    create(_.toLong)

  implicit val forString: LongKey[String] =
    forInt.contramap(MurmurHash3.stringHash)

  implicit val forProjectPath: LongKey[projects.Path] =
    forString.contramap(_.value)

  implicit val forProject: LongKey[Project] =
    forProjectPath.contramap(_.path)
}

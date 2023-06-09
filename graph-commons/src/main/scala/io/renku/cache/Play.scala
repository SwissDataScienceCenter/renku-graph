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
import scala.concurrent.duration._

object Play extends IOApp.Simple {

  val cacheResource = Cache.memoryAsync[IO, Int, String](5, EvictStrategy.LeastUsed, true)

  val calc: Int => IO[Option[String]] = n => IO.sleep(0.1.seconds).as(n.toString.some)

  def test(cache: Cache[IO, Int, String]) = {
    val f = cache.withCache(calc)
    (List.range(0, 26) ++ List.range(2, 8)).traverse(f)
  }

  override def run: IO[Unit] =
    cacheResource.use { cache =>
      fs2.Stream
        .repeatEval(
          test(cache).flatTap(IO.println) *> IO.sleep(1.seconds)
        )
        .take(5)
        .compile
        .drain
    }

}

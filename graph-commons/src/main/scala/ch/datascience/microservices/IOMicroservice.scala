/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.microservices

import java.util.concurrent.ConcurrentHashMap

import cats.effect.{CancelToken, IO, IOApp, Resource}

import scala.collection.JavaConverters._

trait IOMicroservice extends IOApp {

  protected val subProcessesCancelTokens = new ConcurrentHashMap[CancelToken[IO], Unit]()

  def stopSubProcesses: List[CancelToken[IO]] = subProcessesCancelTokens.keys().asScala.toList

  implicit class IOLiftOps[T](monad: IO[T]) {
    lazy val asResource: Resource[IO, T] = Resource.liftF(monad)
  }
}

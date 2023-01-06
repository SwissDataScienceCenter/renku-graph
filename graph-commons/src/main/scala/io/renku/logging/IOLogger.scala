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

package io.renku.logging

import cats.effect._
import org.slf4j.{Logger => Slf4jLogger}
import org.typelevel.log4cats.Logger

class IOLogger(logger: Slf4jLogger) extends Logger[IO] {

  override def error(t: Throwable)(message: => String): IO[Unit] = IO.delay {
    logger.error(message, t)
  }

  override def warn(t: Throwable)(message: => String): IO[Unit] = IO.delay {
    logger.warn(message, t)
  }

  override def info(t: Throwable)(message: => String): IO[Unit] = IO.delay {
    logger.info(message, t)
  }

  override def debug(t: Throwable)(message: => String): IO[Unit] = IO.delay {
    logger.debug(message, t)
  }

  override def trace(t: Throwable)(message: => String): IO[Unit] = IO.delay {
    logger.trace(message, t)
  }

  override def error(message: => String): IO[Unit] = IO.delay {
    logger.error(message)
  }

  override def warn(message: => String): IO[Unit] = IO.delay {
    logger.warn(message)
  }

  override def info(message: => String): IO[Unit] = IO.delay {
    logger.info(message)
  }

  override def debug(message: => String): IO[Unit] = IO.delay {
    logger.debug(message)
  }

  override def trace(message: => String): IO[Unit] = IO.delay {
    logger.trace(message)
  }
}

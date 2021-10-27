/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger => Slf4jLogger}

class IOLoggerSpec extends AnyWordSpec with IOSpec with MockFactory with should.Matchers {

  "error(Throwable)(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .error(_: String, _: Throwable))
        .expects(message, exception)

      logger.error(exception)(message).unsafeRunSync()
    }
  }

  "error(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .error(_: String))
        .expects(message)

      logger.error(message).unsafeRunSync()
    }
  }

  "warn(Throwable)(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .warn(_: String, _: Throwable))
        .expects(message, exception)

      logger.warn(exception)(message).unsafeRunSync()
    }
  }

  "warn(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .warn(_: String))
        .expects(message)

      logger.warn(message).unsafeRunSync()
    }
  }

  "info(Throwable)(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .info(_: String, _: Throwable))
        .expects(message, exception)

      logger.info(exception)(message).unsafeRunSync()
    }
  }

  "info(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .info(_: String))
        .expects(message)

      logger.info(message).unsafeRunSync()
    }
  }

  "debug(Throwable)(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .debug(_: String, _: Throwable))
        .expects(message, exception)

      logger.debug(exception)(message).unsafeRunSync()
    }
  }

  "debug(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .debug(_: String))
        .expects(message)

      logger.debug(message).unsafeRunSync()
    }
  }

  "trace(Throwable)(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .trace(_: String, _: Throwable))
        .expects(message, exception)

      logger.trace(exception)(message).unsafeRunSync()
    }
  }

  "trace(String)" should {
    "call relevant method on the underlying logger" in new TestCase {
      (underlyingLogger
        .trace(_: String))
        .expects(message)

      logger.trace(message).unsafeRunSync()
    }
  }

  private trait TestCase {
    val exception = exceptions.generateOne
    val message   = nonEmptyStrings().generateOne

    val underlyingLogger = mock[Slf4jLogger]
    val logger           = new IOLogger(underlyingLogger)
  }
}

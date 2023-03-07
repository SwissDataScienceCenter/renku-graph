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

package io.renku.events.consumers

import cats.effect.{IO, Ref}
import io.renku.events.consumers.EventSchedulingResult.{Accepted, SchedulingError}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.exceptions
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class SequentialProcessExecutorSpec extends AnyWordSpec with should.Matchers with IOSpec {

  "tryExecuting" should {

    "evaluate the given process and return Accepted" in {

      val value = Ref.unsafe[IO, Boolean](false)

      val process = value.set(true)

      value.get.unsafeRunSync() shouldBe false

      ProcessExecutor.sequential[IO].tryExecuting(process).unsafeRunSync() shouldBe Accepted

      value.get.unsafeRunSync() shouldBe true
    }

    "return the SchedulingError in case the given process fails" in {

      val exception = exceptions.generateOne
      val process   = IO.raiseError(exception)

      ProcessExecutor.sequential[IO].tryExecuting(process).unsafeRunSync() shouldBe SchedulingError(exception)
    }
  }
}

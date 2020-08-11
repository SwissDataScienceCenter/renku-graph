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

package ch.datascience.tinytypes.constraints

import java.time.{Clock, Instant, ZoneId}

import ch.datascience.generators.Generators._
import ch.datascience.tinytypes.{InstantTinyType, TinyTypeFactory}
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class InstantNotInTheFutureSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "InstantNotInTheFuture" should {

    "be instantiatable when values are Instants in the past" in {
      forAll(timestampsNotInTheFuture) { someValue =>
        InstantInThePastType(someValue).value shouldBe someValue
      }
    }

    "be instantiatable when values are Instants from now" in {
      val systemZone = ZoneId.systemDefault
      val fixedNow   = Instant.now

      InstantNotInTheFutureType.clock = Clock.fixed(fixedNow, systemZone)

      InstantInThePastType(fixedNow).value shouldBe fixedNow

      InstantNotInTheFutureType.clock = Clock.system(systemZone)
    }

    "throw an IllegalArgumentException for instants from the future" in {
      intercept[IllegalArgumentException] {
        InstantNotInTheFutureType(Instant.now().plusSeconds(10))
      }.getMessage shouldBe "ch.datascience.tinytypes.constraints.InstantNotInTheFutureType cannot be in the future"
    }
  }
}

private class InstantNotInTheFutureType private (val value: Instant) extends AnyVal with InstantTinyType

private object InstantNotInTheFutureType
    extends TinyTypeFactory[InstantNotInTheFutureType](new InstantNotInTheFutureType(_))
    with InstantNotInTheFuture {

  var clock:                  Clock   = Clock.systemDefaultZone()
  protected override def now: Instant = clock.instant()
}

/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class InstantInThePastSpec extends WordSpec with ScalaCheckPropertyChecks {

  "InstantInThePast" should {

    "be instantiatable when values are Instants in the past" in {
      forAll(timestampsNotInTheFuture) { someValue =>
        InstantInThePastType(someValue).value shouldBe someValue
      }
    }

    "throw an IllegalArgumentException for instants the future" in {
      intercept[IllegalArgumentException] {
        InstantInThePastType(Instant.now().plusSeconds(10))
      }.getMessage shouldBe "ch.datascience.tinytypes.constraints.InstantInThePastType has to be in the past"
    }

    "throw an IllegalArgumentException for instant equal to Instant.now" in {
      val systemZone = ZoneId.systemDefault
      val fixedNow   = Instant.now

      InstantInThePastType.clock = Clock.fixed(fixedNow, systemZone)

      intercept[IllegalArgumentException] {
        InstantInThePastType(fixedNow)
      }.getMessage shouldBe "ch.datascience.tinytypes.constraints.InstantInThePastType has to be in the past"

      InstantInThePastType.clock = Clock.system(systemZone)
    }
  }
}

private class InstantInThePastType private (val value: Instant) extends AnyVal with InstantTinyType

private object InstantInThePastType
    extends TinyTypeFactory[InstantInThePastType](new InstantInThePastType(_))
    with InstantInThePast {

  var clock:                  Clock   = Clock.systemDefaultZone()
  protected override def now: Instant = clock.instant()
}

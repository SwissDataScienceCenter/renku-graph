/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku

import io.renku.control.{RateLimit, RateLimitUnit}
import io.renku.data.Message
import io.renku.generators.Generators
import io.renku.logging.ExecutionTimeRecorder.ElapsedTime
import io.renku.microservices.{MicroserviceBaseUrl, MicroserviceIdentifier}
import org.scalacheck.{Arbitrary, Gen}

trait CommonGenerators {

  val elapsedTimes: Gen[ElapsedTime] = Gen.choose(0L, 10000L) map ElapsedTime.apply

  def rateLimits[Target]: Gen[RateLimit[Target]] = for {
    items <- Generators.positiveLongs()
    unit  <- Gen.oneOf(RateLimitUnit.Second, RateLimitUnit.Minute, RateLimitUnit.Hour, RateLimitUnit.Day)
  } yield RateLimit[Target](items, per = unit)

  implicit val microserviceBaseUrls: Gen[MicroserviceBaseUrl] = for {
    protocol <- Arbitrary.arbBool.arbitrary map {
                  case true  => "http"
                  case false => "https"
                }
    port <- Generators.httpPorts
    ip1  <- Generators.positiveInts(999)
    ip2  <- Generators.positiveInts(999)
    ip3  <- Generators.positiveInts(999)
    ip4  <- Generators.positiveInts(999)
  } yield MicroserviceBaseUrl(s"$protocol://$ip1$ip2$ip3$ip4:$port")

  implicit val microserviceIdentifiers: Gen[MicroserviceIdentifier] =
    Gen.uuid map (_ => MicroserviceIdentifier.generate)

  val errorMessages: Gen[Message] = Gen.oneOf(
    Generators.nonBlankStrings().map(Message.Error(_)),
    Generators.exceptions.map(Message.Error.fromExceptionMessage(_)),
    Generators.jsons.map(Message.Error.fromJsonUnsafe)
  )

  val infoMessages: Gen[Message] =
    Generators.nonBlankStrings().map(Message.Info(_))

  implicit val messages: Gen[Message] =
    Gen.oneOf(errorMessages, infoMessages)
}

object CommonGenerators extends CommonGenerators

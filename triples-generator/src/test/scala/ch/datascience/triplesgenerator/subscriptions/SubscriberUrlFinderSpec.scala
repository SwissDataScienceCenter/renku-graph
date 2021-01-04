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

package ch.datascience.triplesgenerator.subscriptions

import java.net.NetworkInterface

import cats.syntax.all._
import ch.datascience.triplesgenerator.Microservice
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._
import scala.util.Try

class SubscriberUrlFinderSpec extends AnyWordSpec with should.Matchers {

  "findSubscriberUrl" should {

    "return host IP" in new TestCase {
      finder.findSubscriberUrl() shouldBe SubscriberUrl(
        s"http:/$findAddress:${Microservice.ServicePort}/events"
      ).pure[Try]
    }
  }

  private trait TestCase {
    val finder = new SubscriptionUrlFinderImpl[Try]()
  }

  private def findAddress = {
    val ipAddresses = NetworkInterface.getNetworkInterfaces.asScala.toSeq.flatMap(p => p.getInetAddresses.asScala.toSeq)
    ipAddresses
      .find { address =>
        address.getHostAddress.contains(".") &&
        !address.isLoopbackAddress &&
        !address.isAnyLocalAddress &&
        !address.isLinkLocalAddress
      } getOrElse fail("Cannot find machine IP")
  }
}

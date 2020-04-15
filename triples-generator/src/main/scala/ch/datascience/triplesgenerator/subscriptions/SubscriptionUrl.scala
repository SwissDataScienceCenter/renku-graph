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

package ch.datascience.triplesgenerator.subscriptions

import cats.MonadError
import ch.datascience.tinytypes.constraints.Url
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import ch.datascience.triplesgenerator.Microservice

import scala.language.higherKinds

final class SubscriptionUrl private (val value: String) extends AnyVal with StringTinyType
object SubscriptionUrl extends TinyTypeFactory[SubscriptionUrl](new SubscriptionUrl(_)) with Url

private trait SubscriptionUrlFinder[Interpretation[_]] {
  def findSubscriptionUrl: Interpretation[SubscriptionUrl]
}

private class SubscriptionUrlFinderImpl[Interpretation[_]]()(implicit ME: MonadError[Interpretation, Throwable])
    extends SubscriptionUrlFinder[Interpretation] {

  import java.net.NetworkInterface

  import cats.implicits._

  import scala.collection.JavaConverters._

  override def findSubscriptionUrl: Interpretation[SubscriptionUrl] =
    findAddress flatMap {
      case None =>
        new Exception("Cannot find service IP").raiseError[Interpretation, SubscriptionUrl]
      case Some(address) =>
        SubscriptionUrl(s"http://${address.getHostAddress}:${Microservice.ServicePort}/events").pure[Interpretation]
    }

  private def findAddress = ME.catchNonFatal {
    val ipAddresses = NetworkInterface.getNetworkInterfaces.asScala.toSeq.flatMap(p => p.getInetAddresses.asScala.toSeq)
    ipAddresses
      .find { address =>
        address.getHostAddress.contains(".") &&
        !address.isLoopbackAddress &&
        !address.isAnyLocalAddress &&
        !address.isLinkLocalAddress
      }
  }
}

private object IOSubscriptionUrlFinder {
  import cats.effect.IO

  def apply(): IO[SubscriptionUrlFinder[IO]] = IO {
    new SubscriptionUrlFinderImpl[IO]()
  }
}

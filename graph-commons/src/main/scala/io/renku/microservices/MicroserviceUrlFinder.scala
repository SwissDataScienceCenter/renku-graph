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

package io.renku.microservices

import cats.MonadThrow
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.Positive
import io.circe.Decoder
import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.json.TinyTypeDecoders.urlDecoder
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}

trait MicroserviceUrlFinder[F[_]] {
  def findBaseUrl(): F[MicroserviceBaseUrl]
}

class MicroserviceUrlFinderImpl[F[_]: MonadThrow](microservicePort: Int Refined Positive)
    extends MicroserviceUrlFinder[F] {

  import cats.syntax.all._

  import java.net.NetworkInterface
  import scala.jdk.CollectionConverters._

  override def findBaseUrl(): F[MicroserviceBaseUrl] = findAddress >>= {
    case Some(address) => MicroserviceBaseUrl(s"http://${address.getHostAddress}:$microservicePort").pure[F]
    case None          => new Exception("Cannot find service IP").raiseError[F, MicroserviceBaseUrl]
  }

  private def findAddress = MonadThrow[F].catchNonFatal {
    val ipAddresses = NetworkInterface.getNetworkInterfaces.asScala.toSeq.flatMap(_.getInetAddresses.asScala.toSeq)
    ipAddresses
      .find { address =>
        address.getHostAddress.contains(".") &&
        !address.isLoopbackAddress &&
        !address.isAnyLocalAddress &&
        !address.isLinkLocalAddress
      }
  }
}

object MicroserviceUrlFinder {

  def apply[F[_]: MonadThrow](microservicePort: Int Refined Positive): F[MicroserviceUrlFinder[F]] =
    MonadThrow[F].catchNonFatal {
      new MicroserviceUrlFinderImpl[F](microservicePort)
    }
}

final class MicroserviceBaseUrl private (val value: String) extends AnyVal with UrlTinyType
object MicroserviceBaseUrl
    extends TinyTypeFactory[MicroserviceBaseUrl](new MicroserviceBaseUrl(_))
    with Url
    with UrlOps[MicroserviceBaseUrl]
    with TinyTypeJsonLDOps[MicroserviceBaseUrl] {
  implicit val decoder: Decoder[MicroserviceBaseUrl] = urlDecoder(MicroserviceBaseUrl)
}

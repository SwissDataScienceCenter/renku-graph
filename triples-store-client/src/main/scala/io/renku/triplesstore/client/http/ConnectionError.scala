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

package io.renku.triplesstore.client.http

import org.http4s.client.ConnectionFailure
import org.http4s.ember.core.EmberException

import java.io.IOException
import java.net.{ConnectException, SocketException, UnknownHostException}
import java.nio.channels.ClosedChannelException

object ConnectionError {

  def exists(ex: Throwable): Boolean =
    unapply(ex).isDefined

  def unapply(ex: Throwable): Option[Throwable] =
    ex match {
      case _: ConnectionFailure | _: ConnectException | _: SocketException | _: UnknownHostException =>
        Some(ex)
      case _: IOException
          if ex.getMessage.toLowerCase
            .contains("connection reset") || ex.getMessage.toLowerCase.contains("broken pipe") =>
        Some(ex)
      case _: EmberException.ReachedEndOfStream => Some(ex)
      case _: ClosedChannelException            => Some(ex)
      case _ => None
    }
}

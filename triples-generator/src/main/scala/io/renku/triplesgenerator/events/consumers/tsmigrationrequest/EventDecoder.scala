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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.syntax.all._
import io.circe.Json
import io.renku.config.ServiceVersion
import io.renku.events.EventRequestContent

private trait EventDecoder {
  def decode(serviceVersion: ServiceVersion): EventRequestContent => Either[Exception, Unit]
}

private object EventDecoder extends EventDecoder {

  override def decode(serviceVersion: ServiceVersion): EventRequestContent => Either[Exception, Unit] = {
    case EventRequestContent.NoPayload(event: Json) => (decodeVersion andThenF checkMatch(serviceVersion))(event)
    case _                                          => new Exception("Invalid event").asLeft
  }

  private lazy val decodeVersion: Json => Either[Exception, ServiceVersion] =
    _.hcursor.downField("subscriber").downField("version").as[ServiceVersion]

  private def checkMatch(serviceVersion: ServiceVersion): ServiceVersion => Either[Exception, Unit] = {
    case `serviceVersion` => ().asRight
    case version          => new Exception(show"Service in version '$serviceVersion' but event for '$version'").asLeft
  }
}

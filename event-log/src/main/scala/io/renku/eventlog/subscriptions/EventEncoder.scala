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

package io.renku.eventlog.subscriptions

import ch.datascience.http.client.RestClient.PartEncoder
import io.circe.Json
import org.http4s.multipart.Part

private trait EventEncoder[CategoryEvent] {
  def encodeParts[Interpretation[_]](event: CategoryEvent): Vector[Part[Interpretation]]
}

private object EventEncoder {
  def apply[CategoryEvent](
      eventEncoder:           CategoryEvent => Json
  )(implicit jsonPartEncoder: PartEncoder[Json]): EventEncoder[CategoryEvent] =
    new EventEncoder[CategoryEvent] {
      def encodeParts[Interpretation[_]](event: CategoryEvent): Vector[Part[Interpretation]] = Vector(
        jsonPartEncoder.encode("event", eventEncoder(event))
      )
    }

  def apply[CategoryEvent, PayloadType](
      eventEncoder:   CategoryEvent => Json,
      payloadEncoder: CategoryEvent => PayloadType
  )(implicit
      jsonPartEncoder:    PartEncoder[Json],
      payloadPartEncoder: PartEncoder[PayloadType]
  ): EventEncoder[CategoryEvent] =
    new EventEncoder[CategoryEvent] {
      def encodeParts[Interpretation[_]](event: CategoryEvent): Vector[Part[Interpretation]] = Vector(
        jsonPartEncoder.encode("event", eventEncoder(event)),
        payloadPartEncoder.encode("payload", payloadEncoder(event))
      )
    }
}

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

package io.renku.triplesgenerator.events.categories.triplesgenerated

import cats.MonadThrow
import cats.effect.Sync
import cats.syntax.all._
import io.renku.compression.Zip
import io.renku.events.consumers.Project
import io.renku.graph.model.events.{CompoundEventId, ZippedEventPayload}
import io.renku.jsonld.parser.ParsingFailure
import io.renku.jsonld.{JsonLD, parser}

import scala.util.control.NonFatal

private trait EventBodyDeserializer[F[_]] {
  def toEvent(eventId: CompoundEventId, project: Project, zippedPayload: ZippedEventPayload): F[TriplesGeneratedEvent]
}

private class EventBodyDeserializerImpl[F[_]: Sync](zip: Zip = Zip) extends EventBodyDeserializer[F] {

  override def toEvent(eventId:       CompoundEventId,
                       project:       Project,
                       zippedPayload: ZippedEventPayload
  ): F[TriplesGeneratedEvent] = for {
    unzipped <- zip.unzip[F](zippedPayload.value)
    payload <- MonadThrow[F].fromEither(parser.parse(unzipped)).recoverWith { case NonFatal(error) =>
                 ParsingFailure(s"TriplesGeneratedEvent cannot be deserialised: $eventId", error)
                   .raiseError[F, JsonLD]
               }
  } yield TriplesGeneratedEvent(eventId.id, project, payload)
}

private object EventBodyDeserializer {
  def apply[F[_]: Sync]: EventBodyDeserializer[F] = new EventBodyDeserializerImpl[F]
}

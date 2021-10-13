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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated

import cats.MonadThrow
import cats.effect.{BracketThrow, IO, Sync}
import cats.syntax.all._
import ch.datascience.compression.Zip
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.events.{CompoundEventId, ZippedEventPayload}
import io.renku.jsonld.parser.ParsingFailure
import io.renku.jsonld.{JsonLD, parser}

import scala.util.control.NonFatal

private trait EventBodyDeserializer[Interpretation[_]] {
  def toEvent(eventId:       CompoundEventId,
              project:       Project,
              zippedPayload: ZippedEventPayload
  ): Interpretation[TriplesGeneratedEvent]
}

private class EventBodyDeserializerImpl[Interpretation[_]: BracketThrow: Sync](zip: Zip = Zip)
    extends EventBodyDeserializer[Interpretation] {

  override def toEvent(eventId:       CompoundEventId,
                       project:       Project,
                       zippedPayload: ZippedEventPayload
  ): Interpretation[TriplesGeneratedEvent] = for {
    unzipped <- zip.unzip[Interpretation](zippedPayload.value)
    payload <- MonadThrow[Interpretation].fromEither(parser.parse(unzipped)).recoverWith { case NonFatal(error) =>
                 ParsingFailure(s"TriplesGeneratedEvent cannot be deserialised: $eventId", error)
                   .raiseError[Interpretation, JsonLD]
               }
  } yield TriplesGeneratedEvent(eventId.id, project, payload)
}

private object EventBodyDeserializer {
  def apply(): EventBodyDeserializer[IO] = new EventBodyDeserializerImpl[IO]
}

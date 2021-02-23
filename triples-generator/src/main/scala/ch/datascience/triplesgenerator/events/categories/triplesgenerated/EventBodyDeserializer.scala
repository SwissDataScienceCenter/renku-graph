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

import cats.MonadError
import cats.effect.IO
import cats.syntax.all._
import ch.datascience.events.consumers.Project
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events._
import ch.datascience.rdfstore.JsonLDTriples
import io.circe.parser._
import io.circe.{DecodingFailure, Error, ParsingFailure}

private trait EventBodyDeserializer[Interpretation[_]] {
  def toTriplesGeneratedEvent(eventId:       CompoundEventId,
                              project:       Project,
                              schemaVersion: SchemaVersion,
                              eventBody:     EventBody
  ): Interpretation[TriplesGeneratedEvent]
}

private class EventBodyDeserializerImpl[Interpretation[_]](implicit
    ME: MonadError[Interpretation, Throwable]
) extends EventBodyDeserializer[Interpretation] {

  override def toTriplesGeneratedEvent(eventId:       CompoundEventId,
                                       project:       Project,
                                       schemaVersion: SchemaVersion,
                                       eventBody:     EventBody
  ): Interpretation[TriplesGeneratedEvent] =
    ME.fromEither {
      parse(eventBody.value)
        .map(json => TriplesGeneratedEvent(eventId.id, project, JsonLDTriples(json), schemaVersion))
        .leftMap(toMeaningfulError(eventId))
    }

  private def toMeaningfulError(eventId: CompoundEventId): Error => Error = {
    case failure: DecodingFailure => failure.withMessage(s"TriplesGeneratedEvent cannot be deserialised: $eventId")
    case failure: ParsingFailure =>
      ParsingFailure(s"TriplesGeneratedEvent cannot be deserialised: $eventId", failure)
  }
}

private object EventBodyDeserializer {
  def apply(): EventBodyDeserializer[IO] = new EventBodyDeserializerImpl[IO]
}

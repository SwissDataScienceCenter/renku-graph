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

package io.renku.triplesgenerator.events.consumers.cleanup

import cats.syntax.all._
import io.circe.{DecodingFailure, Error, Json}
import io.renku.events.EventRequestContent

private trait EventDecoder {
  val decode: EventRequestContent => Either[Exception, CleanUpEvent]
}

private object EventDecoder extends EventDecoder {

  import io.renku.events.consumers.EventDecodingTools._

  lazy val decode: EventRequestContent => Either[Exception, CleanUpEvent] = req =>
    req.event.getProject.map(CleanUpEvent).leftMap(toMeaningfulError(req.event))

  private def toMeaningfulError(event: Json): DecodingFailure => Error = { failure =>
    failure.withMessage(s"CleanUpEvent cannot be decoded: '$event'")
  }
}

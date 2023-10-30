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

package io.renku.eventsqueue

import cats.syntax.all._
import io.circe.syntax._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{countingGen, nonEmptyStrings}
import org.scalacheck.Gen
import skunk.data.Identifier

object Generators {

  val dequeuedEvents: Gen[DequeuedEvent] =
    (countingGen.map(_.toInt), events.map(_.asJson.noSpaces))
      .mapN(DequeuedEvent.apply)

  private[eventsqueue] lazy val events: Gen[TestEvent] =
    nonEmptyStrings().map(TestEvent(_))

  def channelIds: Gen[Identifier] =
    nonEmptyStrings(minLength = 5)
      .map(_.toLowerCase)
      .map(Identifier.fromString)
      .map(_.fold(err => throw new Exception(s"Error when generating Channel identifier: $err"), identity))
}

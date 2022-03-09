/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.events

import io.renku.events
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{jsons, nonBlankStrings, nonEmptyStrings}
import io.renku.tinytypes.ByteArrayTinyType
import io.renku.tinytypes.contenttypes.ZippedContent
import org.scalacheck.Gen._
import org.scalacheck.{Arbitrary, Gen}

object Generators {

  val categoryNames: Gen[CategoryName] = nonBlankStrings() map (value => CategoryName(value.value))

  final case class ZippedContentTinyType(value: Array[Byte]) extends ByteArrayTinyType with ZippedContent

  private val zippedContents: Gen[ByteArrayTinyType with ZippedContent] =
    Arbitrary.arbByte.arbitrary
      .toGeneratorOfList()
      .map(_.toArray)
      .generateAs(ZippedContentTinyType.apply)

  implicit val eventRequestContents: Gen[EventRequestContent] = for {
    event        <- jsons
    maybePayload <- oneOf(nonEmptyStrings(), zippedContents).toGeneratorOfOptions
  } yield maybePayload match {
    case Some(payload) => events.EventRequestContent.WithPayload(event, payload)
    case None          => events.EventRequestContent.NoPayload(event)
  }

  implicit val eventRequestContentNoPayloads: Gen[EventRequestContent.NoPayload] =
    jsons map events.EventRequestContent.NoPayload

  implicit val eventRequestContentWithZippedPayloads
      : Gen[EventRequestContent.WithPayload[ByteArrayTinyType with ZippedContent]] = for {
    event   <- jsons
    payload <- zippedContents
  } yield events.EventRequestContent.WithPayload(event, payload)
}

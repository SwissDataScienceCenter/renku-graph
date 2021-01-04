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

package ch.datascience.triplesgenerator.eventprocessing

import cats.MonadError
import cats.data.NonEmptyList
import cats.syntax.all._
import ch.datascience.graph.model.events._
import ch.datascience.graph.model.projects.{Id, Path}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.triplesgenerator.eventprocessing.CommitEvent.{CommitEventWithParent, CommitEventWithoutParent}
import io.circe.parser._
import io.circe.{Decoder, DecodingFailure, Error, HCursor, ParsingFailure}

private class EventBodyDeserialiser[Interpretation[_]](implicit
    ME: MonadError[Interpretation, Throwable]
) {

  def toCommitEvents(eventBody: EventBody): Interpretation[NonEmptyList[CommitEvent]] = ME.fromEither {
    parse(eventBody.value)
      .flatMap(_.as[NonEmptyList[CommitEvent]])
      .leftMap(toMeaningfulError(eventBody))
  }

  private implicit val commitsDecoder: Decoder[NonEmptyList[CommitEvent]] = (cursor: HCursor) =>
    for {
      commitId      <- cursor.downField("id").as[CommitId]
      projectId     <- cursor.downField("project").downField("id").as[Id]
      projectPath   <- cursor.downField("project").downField("path").as[Path]
      parentCommits <- cursor.downField("parents").as[List[CommitId]]
    } yield {
      val project = Project(projectId, projectPath)
      parentCommits match {
        case Nil =>
          NonEmptyList.one(CommitEventWithoutParent(EventId(commitId.value), project, commitId))
        case parentIds =>
          NonEmptyList.fromListUnsafe(
            parentIds map (CommitEventWithParent(EventId(commitId.value), project, commitId, _))
          )
      }
    }

  private def toMeaningfulError(eventBody: EventBody): Error => Error = {
    case failure: DecodingFailure => failure.withMessage(s"CommitEvent cannot be deserialised: '$eventBody'")
    case failure: ParsingFailure  => ParsingFailure(s"CommitEvent cannot be deserialised: '$eventBody'", failure)
  }
}

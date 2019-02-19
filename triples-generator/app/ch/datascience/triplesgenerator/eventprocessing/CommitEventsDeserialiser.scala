/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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
import cats.implicits._
import ch.datascience.graph.events.{CommitId, ProjectPath}
import ch.datascience.triplesgenerator.eventprocessing.Commit.{CommitWithParent, CommitWithoutParent}
import io.circe.parser._
import io.circe.{Decoder, DecodingFailure, Error, HCursor, ParsingFailure}

import scala.language.higherKinds

private class CommitEventsDeserialiser[Interpretation[_]](
    implicit ME: MonadError[Interpretation, Throwable]
) {

  def deserialiseToCommitEvents(jsonString: String): Interpretation[List[Commit]] = ME.fromEither {
    parse(jsonString)
      .flatMap(_.as[List[Commit]])
      .leftMap(toMeaningfulError(jsonString))
  }

  private implicit val commitsDecoder: Decoder[List[Commit]] = (cursor: HCursor) =>
    for {
      commitId      <- cursor.downField("id").as[CommitId]
      projectPath   <- cursor.downField("project").downField("path").as[ProjectPath]
      parentCommits <- cursor.downField("parents").as[List[CommitId]]
    } yield
      parentCommits match {
        case Nil       => List(CommitWithoutParent(commitId, projectPath))
        case parentIds => parentIds map (CommitWithParent(commitId, _, projectPath))
    }

  private def toMeaningfulError(json: String): Error => Error = {
    case failure: DecodingFailure => failure.withMessage(s"CommitEvent cannot be deserialised: '$json'")
    case failure: ParsingFailure  => ParsingFailure(s"CommitEvent cannot be deserialised: '$json'", failure)
  }
}

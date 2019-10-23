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

package ch.datascience.triplesgenerator.reprovisioning

import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.ProjectPath
import ch.datascience.tinytypes.{StringTinyType, TinyTypeConverter, TinyTypeFactory}
import io.circe.Decoder

import scala.language.higherKinds

private final case class OutdatedTriples(projectResource: ProjectResource, commits: Set[CommitIdResource])

final class ProjectResource private (val value: String) extends AnyVal with StringTinyType
object ProjectResource extends TinyTypeFactory[ProjectResource](new ProjectResource(_)) {
  implicit lazy val projectResourceDecoder: Decoder[ProjectResource] = Decoder.decodeString.map(ProjectResource.apply)

  private val pathExtractor = "^.*\\/(.*\\/.*)$".r
  implicit lazy val projectPathConverter: TinyTypeConverter[ProjectResource, ProjectPath] = {
    case ProjectResource(pathExtractor(path)) => ProjectPath.from(path)
    case illegalValue                         => Left(new IllegalArgumentException(s"'$illegalValue' cannot be converted to a ProjectPath"))
  }
}

final class CommitIdResource private (val value: String) extends AnyVal with StringTinyType
object CommitIdResource extends TinyTypeFactory[CommitIdResource](new CommitIdResource(_)) {
  factory =>

  private val validationRegex = ".*\\/commit\\/([0-9a-f]{5,40})\\/?.*".r

  addConstraint(
    check   = validationRegex.findFirstMatchIn(_).isDefined,
    message = (value: String) => s"'$value' is not a valid Commit Id Resource"
  )

  implicit lazy val commitIdDecoder: Decoder[CommitIdResource] = Decoder.decodeString.map(CommitIdResource.apply)

  implicit lazy val commitIdConverter: TinyTypeConverter[CommitIdResource, CommitId] = {
    case CommitIdResource(factory.validationRegex(commitId)) => CommitId.from(commitId)
    case illegalValue                                        => Left(new IllegalArgumentException(s"'$illegalValue' cannot be converted to CommitId"))
  }
}

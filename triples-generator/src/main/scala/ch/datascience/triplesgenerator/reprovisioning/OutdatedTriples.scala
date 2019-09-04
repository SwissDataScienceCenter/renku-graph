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

import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.tinytypes.constraints.Url
import ch.datascience.tinytypes.{Renderer, StringTinyType, TinyTypeFactory}
import io.circe.Decoder

import scala.language.higherKinds

private final case class OutdatedTriples(projectPath: FullProjectPath, commits: Set[CommitIdResource])

class FullProjectPath private (val value: String) extends AnyVal with StringTinyType
object FullProjectPath extends TinyTypeFactory[FullProjectPath](new FullProjectPath(_)) with Url {

  def from(renkuBaseUrl: RenkuBaseUrl, projectPath: ProjectPath): FullProjectPath =
    FullProjectPath((renkuBaseUrl / projectPath).value)

  implicit lazy val projectPathDecoder: Decoder[FullProjectPath] = Decoder.decodeString.map(FullProjectPath.apply)

  implicit lazy val projectPathConverter: FullProjectPath => Either[Exception, ProjectPath] = {
    val projectPathExtractor = "^.*\\/(.*\\/.*)$".r

    {
      case FullProjectPath(projectPathExtractor(path)) => ProjectPath.from(path)
      case illegalValue                                => Left(new IllegalArgumentException(s"'$illegalValue' cannot be converted to a ProjectPath"))
    }
  }

  implicit lazy val rdfResourceRenderer: Renderer[RdfResource, FullProjectPath] =
    value => s"<$value>"
}

final class CommitIdResource private (val value: String) extends AnyVal with StringTinyType
object CommitIdResource extends TinyTypeFactory[CommitIdResource](new CommitIdResource(_)) {
  factory =>

  private val validationRegex = "^file:\\/\\/\\/commit\\/([0-9a-f]{5,40})\\/?.*$".r

  addConstraint(
    check   = validationRegex.findFirstMatchIn(_).isDefined,
    message = (value: String) => s"'$value' is not a valid Commit Id Resource"
  )

  implicit lazy val commitIdDecoder: Decoder[CommitIdResource] = Decoder.decodeString.map(CommitIdResource.apply)

  implicit lazy val commitIdConverter: CommitIdResource => Either[IllegalArgumentException, CommitId] = {
    case CommitIdResource(factory.validationRegex(commitId)) => CommitId.from(commitId)
    case illegalValue                                        => Left(new IllegalArgumentException(s"'$illegalValue' cannot be converted to CommitId"))
  }

  implicit lazy val rdfResourceRenderer: Renderer[RdfResource, CommitIdResource] =
    value => s"<$value>"
}

trait RdfResource

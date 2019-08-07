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

package ch.datascience.knowledgegraph.graphql.lineage

import cats.effect.IO
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.knowledgegraph.graphql.Arguments._
import ch.datascience.knowledgegraph.graphql.QueryContext
import ch.datascience.tinytypes.constraints.RelativePath
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import eu.timepit.refined.auto._
import sangria.schema._

import scala.language.higherKinds

private[graphql] object QueryFields {

  import modelSchema._

  def apply(): List[Field[QueryContext[IO], Unit]] =
    fields[QueryContext[IO], Unit](
      Field(
        name        = "lineage",
        fieldType   = OptionType(lineageType),
        description = Some("Returns a lineage for a project with the given path"),
        arguments   = List(projectPathArgument, commitIdArgument, filePathArgument),
        resolve = context =>
          context.ctx.lineageFinder
            .findLineage(context.args arg projectPathArgument,
                         context.args arg commitIdArgument,
                         context.args arg filePathArgument)
            .unsafeToFuture()
      )
    )

  val projectPathArgument = Argument(
    name = "projectPath",
    argumentType = ProjectPath.toScalarType(
      description      = "Project's path in the GitLab.",
      exceptionMessage = "ProjectPath value expected in format <namespace>/<project>"
    )
  )

  val commitIdArgument = Argument(
    name         = "commitId",
    argumentType = CommitId.toScalarType(description = "Commit Id")
  )

  final class FilePath private (val value: String) extends AnyVal with StringTinyType
  object FilePath extends TinyTypeFactory[FilePath](new FilePath(_)) with RelativePath
  val filePathArgument = Argument(
    name         = "filePath",
    argumentType = FilePath.toScalarType(description = "File path")
  )
}

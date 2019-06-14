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

package ch.datascience.graphservice.graphql.lineage

import cats.effect.IO
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.graphservice.graphql.QueryContext
import ch.datascience.tinytypes.constraints.RelativePath
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import sangria.ast
import sangria.schema._
import sangria.validation.ValueCoercionViolation

import scala.language.higherKinds

private[graphql] object QueryFields {

  import modelSchema._

  def apply(): List[Field[QueryContext[IO], Unit]] =
    fields[QueryContext[IO], Unit](
      Field(
        name        = "lineage",
        fieldType   = OptionType(lineageType),
        description = Some("Returns a lineage for a project with the given path"),
        arguments   = List(ProjectPathArgument, MaybeCommitIdArgument, MaybeFilePathArgument),
        resolve = context =>
          context.ctx.lineageRepository
            .findLineage(context.args arg ProjectPathArgument,
                         context.args arg MaybeCommitIdArgument,
                         context.args arg MaybeFilePathArgument)
            .unsafeToFuture()
      )
    )

  private implicit val ProjectPathType: ScalarType[ProjectPath] = {
    import cats.implicits._
    case object ProjectPathCoercionViolation
        extends ValueCoercionViolation("ProjectPath value expected in format <namespace>/<project>")

    ScalarType[ProjectPath](
      name         = "ProjectPath",
      description  = Some("Project's path in the GitLab."),
      coerceOutput = valueOutput,
      coerceUserInput = {
        case s: String => ProjectPath.from(s) leftMap (_ => ProjectPathCoercionViolation)
        case _ => Left(ProjectPathCoercionViolation)
      },
      coerceInput = {
        case ast.StringValue(s, _, _, _, _) => ProjectPath.from(s) leftMap (_ => ProjectPathCoercionViolation)
        case _                              => Left(ProjectPathCoercionViolation)
      }
    )
  }
  val ProjectPathArgument = Argument("projectPath", ProjectPathType)

  private implicit val CommitIdType: ScalarType[CommitId] = {
    import cats.implicits._
    case object CommitIdCoercionViolation extends ValueCoercionViolation("CommitId not valid")

    ScalarType[CommitId](
      name         = "CommitId",
      description  = Some("Commit id"),
      coerceOutput = valueOutput,
      coerceUserInput = {
        case s: String => CommitId.from(s) leftMap (_ => CommitIdCoercionViolation)
        case _ => Left(CommitIdCoercionViolation)
      },
      coerceInput = {
        case ast.StringValue(s, _, _, _, _) => CommitId.from(s) leftMap (_ => CommitIdCoercionViolation)
        case _                              => Left(CommitIdCoercionViolation)
      }
    )
  }
  val MaybeCommitIdArgument = Argument("commitId", OptionInputType(CommitIdType))

  final class FilePath private (val value: String) extends AnyVal with TinyType[String]
  object FilePath extends TinyTypeFactory[String, FilePath](new FilePath(_)) with RelativePath
  private implicit val FilePathType: ScalarType[FilePath] = {
    import cats.implicits._
    case object FilePathCoercionViolation extends ValueCoercionViolation("FilePath not valid")

    ScalarType[FilePath](
      name         = "FilePath",
      description  = Some("File path"),
      coerceOutput = valueOutput,
      coerceUserInput = {
        case s: String => FilePath.from(s) leftMap (_ => FilePathCoercionViolation)
        case _ => Left(FilePathCoercionViolation)
      },
      coerceInput = {
        case ast.StringValue(s, _, _, _, _) => FilePath.from(s) leftMap (_ => FilePathCoercionViolation)
        case _                              => Left(FilePathCoercionViolation)
      }
    )
  }
  val MaybeFilePathArgument = Argument("filePath", OptionInputType(FilePathType))
}

/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.lineage.graphql

import cats.effect.IO
import ch.datascience.graph.model.projects.FilePath
import ch.datascience.knowledgegraph.graphql.Arguments._
import ch.datascience.knowledgegraph.graphql.CommonQueryFields._
import ch.datascience.knowledgegraph.graphql.QueryContext
import eu.timepit.refined.auto._
import sangria.schema._

import scala.language.higherKinds

object QueryFields {

  import modelSchema._

  def apply(): List[Field[QueryContext[IO], Unit]] =
    fields[QueryContext[IO], Unit](
      Field(
        name        = "lineage",
        fieldType   = OptionType(lineageType),
        description = Some("Returns a lineage for a project with the given path"),
        arguments   = List(projectPathArgument, filePathArgument),
        resolve = context =>
          context.ctx.lineageFinder
            .findLineage(context.args arg projectPathArgument, context.args arg filePathArgument)
            .unsafeToFuture()
      )
    )

  private val filePathArgument = Argument(
    name         = "filePath",
    argumentType = FilePath.toScalarType(name = "FilePath", description = "File path")
  )
}

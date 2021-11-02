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

package io.renku.knowledgegraph.lineage.graphql

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import eu.timepit.refined.auto._
import io.renku.knowledgegraph.graphql.Arguments._
import io.renku.knowledgegraph.graphql.CommonQueryFields._
import io.renku.knowledgegraph.graphql.LineageQueryContext
import io.renku.knowledgegraph.lineage.model.Node.Location
import sangria.schema._

object QueryFields {

  import modelSchema._

  def apply()(implicit runtime: IORuntime): List[Field[LineageQueryContext[IO], Unit]] =
    fields[LineageQueryContext[IO], Unit](
      Field(
        name = "lineage",
        fieldType = OptionType(lineageType),
        description = Some("Returns a lineage for a project with the given path"),
        arguments = List(projectPathArgument, locationArgument),
        resolve = context =>
          context.ctx.lineageFinder
            .find(context.args arg projectPathArgument, context.args arg locationArgument, context.ctx.maybeUser)
            .unsafeToFuture()
      )
    )

  private val locationArgument = Argument(
    name = "filePath",
    argumentType = Location.toScalarType(name = "FilePath", description = "File path")
  )
}

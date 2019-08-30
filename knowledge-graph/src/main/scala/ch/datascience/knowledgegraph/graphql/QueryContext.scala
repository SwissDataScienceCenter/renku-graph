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

package ch.datascience.knowledgegraph.graphql

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.knowledgegraph.datasets.graphql.{IOProjectDatasetsFinder, ProjectDatasetsFinder}
import ch.datascience.knowledgegraph.lineage.{IOLineageFinder, LineageFinder}

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class QueryContext[Interpretation[_]](
    val lineageFinder:  LineageFinder[Interpretation],
    val datasetsFinder: ProjectDatasetsFinder[Interpretation]
)

object IOQueryContext {
  def apply()(implicit executionContext: ExecutionContext,
              contextShift:              ContextShift[IO],
              timer:                     Timer[IO]): IO[QueryContext[IO]] =
    for {
      lineageFinder  <- IOLineageFinder()
      datasetsFinder <- IOProjectDatasetsFinder()
    } yield new QueryContext[IO](lineageFinder, datasetsFinder)
}

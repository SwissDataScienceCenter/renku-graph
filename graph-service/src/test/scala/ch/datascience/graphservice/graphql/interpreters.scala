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

package ch.datascience.graphservice.graphql

import cats.effect.IO
import ch.datascience.graphservice.config.GitLabBaseUrl
import ch.datascience.graphservice.graphql.lineage.LineageFinder
import ch.datascience.graphservice.rdfstore.RDFConnectionResource
import sangria.schema.Schema

import scala.concurrent.ExecutionContext

private class IOLineageFinder(
    rdfConnectionResource: RDFConnectionResource[IO],
    gitLabBaseUrl:         GitLabBaseUrl
) extends LineageFinder[IO](rdfConnectionResource, gitLabBaseUrl)

private class IOQueryRunner(
    schema:                  Schema[QueryContext[IO], Unit],
    repository:              QueryContext[IO]
)(implicit executionContext: ExecutionContext)
    extends QueryRunner[IO, QueryContext[IO]](schema, repository)

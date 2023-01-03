/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.graph.triplesstore

import cats.syntax.all._
import io.renku.triplesstore.{DatasetConfigFile, DatasetConfigFileFactory, DatasetName}

object DatasetTTLs {

  final class ProjectsTTL private (val value: String) extends DatasetConfigFile
  object ProjectsTTL
      extends DatasetConfigFileFactory[ProjectsTTL](DatasetName("projects"),
                                                    new ProjectsTTL(_),
                                                    ttlFileName = "projects-ds.ttl"
      )

  final class MigrationsTTL private (val value: String) extends DatasetConfigFile
  object MigrationsTTL
      extends DatasetConfigFileFactory[MigrationsTTL](DatasetName("migrations"),
                                                      new MigrationsTTL(_),
                                                      ttlFileName = "migrations-ds.ttl"
      )

  val allFactories: List[DatasetConfigFileFactory[_ <: DatasetConfigFile]] = List(ProjectsTTL, MigrationsTTL)

  val allNamesAndConfigs: Either[Exception, List[(DatasetName, DatasetConfigFile)]] =
    allFactories.map(factory => factory.fromTtlFile().map(factory.datasetName -> _)).sequence
}

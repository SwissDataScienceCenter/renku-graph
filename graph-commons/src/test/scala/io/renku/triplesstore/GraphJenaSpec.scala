/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectSlugs
import io.renku.graph.model.RenkuUrl
import io.renku.graph.model.projects.Slug
import io.renku.jsonld.EntityId
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.triplesstore.client.util.JenaSpec
import org.scalatest.Suite
import org.typelevel.log4cats.Logger

trait GraphJenaSpec extends JenaSpec with TestProjectsDataset with TestMigrationsDataset {
  self: Suite =>

  private lazy val defaultProjectForGraph: Slug = projectSlugs.generateOne

  def defaultProjectGraphId(implicit renkuUrl: RenkuUrl): EntityId =
    io.renku.graph.model.testentities.Project.toEntityId(defaultProjectForGraph)

  def tsClient(implicit cc: DatasetConnectionConfig, L: Logger[IO], ioRuntime: IORuntime) = {
    implicit val sqtr: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe
    TSClient[IO](cc)
  }

  def allDSConfigs(implicit L: Logger[IO]): Resource[IO, (ProjectsConnectionConfig, MigrationsConnectionConfig)] =
    projectsDSConfig.flatMap(migrationsDSConfig.tupleLeft(_))
}

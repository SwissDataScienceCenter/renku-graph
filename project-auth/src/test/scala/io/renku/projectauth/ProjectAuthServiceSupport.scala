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

package io.renku.projectauth

import cats.effect._
import fs2.Stream
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.{RenkuUrl, persons}
import io.renku.triplesstore.client.util.JenaContainerSupport
import org.scalacheck.Gen
import org.scalatest.Suite
import org.typelevel.log4cats.Logger

trait ProjectAuthServiceSupport extends JenaContainerSupport { self: Suite =>

  val datasetName: String = "projectauth"

  def withProjectAuthService(implicit renkuUrl: RenkuUrl, L: Logger[IO]): Resource[IO, ProjectAuthService[IO]] =
    withDataset(datasetName).map(ProjectAuthService[IO](_, renkuUrl))

  def withProjectAuthServiceData(data: Stream[Gen, ProjectAuthData])(implicit
      renkuUrl: RenkuUrl,
      L:        Logger[IO]
  ) =
    withProjectAuthService.flatMap { s =>
      Resource.eval(insertData(s, data)).map(data => (s, data))
    }

  def insertData(s: ProjectAuthService[IO], data: Stream[Gen, ProjectAuthData]) = {
    val genData = data.toIO.compile.toList
    genData.flatMap(d => Stream.emits(d).through(s.updateAll).compile.drain.as(d))
  }

  def selectUserFrom(data: List[ProjectAuthData]): Gen[persons.GitLabId] = {
    val userIds = data.flatMap(_.members).map(_.gitLabId).toSet
    Gen.oneOf(userIds)
  }

  def selectUserNotIn(data: List[ProjectAuthData]): Gen[persons.GitLabId] = {
    val userIds = data.flatMap(_.members).map(_.gitLabId).toSet
    Gen
      .oneOf(1000 to 100000)
      .suchThat(id => !userIds.contains(id))
      .map(persons.GitLabId(_))
  }
}

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

package io.renku.tokenrepository.repository.creation

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.tokenrepository.repository.RepositoryGenerators.encryptedAccessTokens
import io.renku.tokenrepository.repository.TokenRepositoryPostgresSpec
import io.renku.tokenrepository.repository.metrics.{QueriesExecutionTimes, TestQueriesExecutionTimes}
import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should

class PersistedSlugFinderSpec
    extends AsyncFlatSpec
    with AsyncIOSpec
    with TokenRepositoryPostgresSpec
    with should.Matchers
    with AsyncMockFactory {

  it should "return Slug for the given project Id" in testDBResource.use { implicit cfg =>
    val projectId   = projectIds.generateOne
    val projectSlug = projectSlugs.generateOne

    insert(projectId, projectSlug, encryptedAccessTokens.generateOne) >>
      new PersistedSlugFinderImpl[IO].findPersistedProjectSlug(projectId).asserting(_ shouldBe Some(projectSlug))
  }

  it should "return None if there's no Slug with the given Id" in testDBResource.use { implicit cfg =>
    new PersistedSlugFinderImpl[IO]
      .findPersistedProjectSlug(projectIds.generateOne)
      .asserting(_ shouldBe None)
  }

  private implicit lazy val qet: QueriesExecutionTimes[IO] = TestQueriesExecutionTimes[IO]
}

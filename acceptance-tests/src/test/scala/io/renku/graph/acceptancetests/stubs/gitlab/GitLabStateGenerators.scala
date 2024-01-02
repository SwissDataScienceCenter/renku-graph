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

package io.renku.graph.acceptancetests.stubs.gitlab

import com.typesafe.config.ConfigFactory
import io.renku.config.ConfigLoader.find
import io.renku.generators.CommonGraphGenerators.projectAccessTokens
import io.renku.generators.Generators
import io.renku.generators.Generators._
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.{CommitData, ProjectAccessTokenInfo}
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.Person
import org.scalacheck.Gen

import java.time.{Instant, LocalDate}
import scala.util.Try

trait GitLabStateGenerators {

  def commitData(commitId: CommitId): Gen[CommitData] =
    for {
      authorName     <- GraphModelGenerators.personNames
      authorEmail    <- GraphModelGenerators.personEmails
      committerName  <- GraphModelGenerators.personNames
      committerEmail <- GraphModelGenerators.personEmails
      message        <- Generators.nonEmptyStrings()
    } yield CommitData(
      commitId,
      Person(authorName, authorEmail),
      Person(committerName, committerEmail),
      Instant.now(),
      message,
      Nil
    )

  def projectAccessTokenInfos: Gen[ProjectAccessTokenInfo] = for {
    id     <- positiveInts()
    userId <- GraphModelGenerators.personGitLabIds
    token  <- projectAccessTokens
    expiry <- localDates(min = LocalDate.now().plusWeeks(1))
  } yield ProjectAccessTokenInfo(
    id.value,
    userId,
    name = find[Try, String]("project-token-name", ConfigFactory.load).fold(throw _, identity),
    token,
    expiry
  )
}

object GitLabStateGenerators extends GitLabStateGenerators

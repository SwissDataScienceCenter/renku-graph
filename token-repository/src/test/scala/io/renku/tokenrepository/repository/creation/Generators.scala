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

package io.renku.tokenrepository.repository
package creation

import RepositoryGenerators._
import TokenDates._
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.projectAccessTokens
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{localDates, timestampsNotInTheFuture}
import io.renku.graph.model.GraphModelGenerators.{projectIds, projectSlugs}
import org.scalacheck.Gen

object Generators {

  val tokenCreatedAts:  Gen[CreatedAt]  = timestampsNotInTheFuture.toGeneratorOf(CreatedAt)
  val tokenExpiryDates: Gen[ExpiryDate] = localDates.toGeneratorOf(ExpiryDate)

  val tokenDates: Gen[TokenDates] =
    (tokenCreatedAts, tokenExpiryDates).mapN(TokenDates.apply)

  val tokenCreationInfos: Gen[TokenCreationInfo] =
    (accessTokenIds, projectAccessTokens, tokenDates).mapN(TokenCreationInfo.apply)

  val projectObjects: Gen[Project] =
    (projectIds, projectSlugs).mapN(Project.apply)

  val tokenStoringInfos: Gen[TokenStoringInfo] =
    (projectObjects, encryptedAccessTokens, tokenDates).mapN(TokenStoringInfo.apply)
}

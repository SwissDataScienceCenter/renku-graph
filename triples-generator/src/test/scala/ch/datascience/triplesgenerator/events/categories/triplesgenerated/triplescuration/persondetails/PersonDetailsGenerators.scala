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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import ch.datascience.generators.Generators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.GitLabId
import ch.datascience.rdfstore.entities
import io.renku.jsonld.syntax._
import org.scalacheck.Gen
import ch.datascience.generators.CommonGraphGenerators._

private object PersonDetailsGenerators {

  implicit val gitLabProjectMembers: Gen[GitLabProjectMember] = for {
    id       <- userGitLabIds
    username <- usernames
    name     <- userNames
  } yield GitLabProjectMember(id, username, name)

  def persons(
      gitLabIdGen:         Gen[Option[GitLabId]] = emptyOptionOf[GitLabId]
  )(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): Gen[Person] = for {
    maybeGitLabId <- gitLabIdGen
    name          <- userNames
    maybeEmail    <- userEmails.toGeneratorOfOptions
    id = users.ResourceId(
           entities
             .Person(name, maybeEmail)
             .asJsonLD
             .entityId
             .getOrElse(throw new Exception("Person resourceId cannot be found"))
         )
  } yield Person(id, maybeGitLabId, name, maybeEmail)

  def persons(
      maybeGitLabId:       Option[GitLabId]
  )(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): Gen[Person] = persons(
    Gen.const(maybeGitLabId)
  )

  implicit val persons: Gen[Person] = persons()(renkuBaseUrls.generateOne, gitLabUrls.generateOne.apiV4)
}

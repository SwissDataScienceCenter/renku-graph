/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.graph.model.testentities
package generators

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.persons.{Email, GitLabId}
import io.renku.tinytypes.InstantTinyType
import org.scalacheck.Gen

import java.time.Instant

object EntitiesGenerators extends EntitiesGenerators {
  type DatasetGenFactory[+P <: Dataset.Provenance] = projects.DateCreated => Gen[Dataset[P]]
  type ActivityGenFactory                          = projects.DateCreated => Gen[Activity]
  type PlanGenFactory                              = projects.DateCreated => Gen[Plan]
}

private object Instances {
  implicit val renkuBaseUrl: RenkuBaseUrl = renkuBaseUrls.generateOne
  implicit val gitLabUrl:    GitLabUrl    = gitLabUrls.generateOne
  implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrl.apiV4
}

trait EntitiesGenerators
    extends RenkuProjectEntitiesGenerators
    with NonRenkuProjectEntitiesGenerators
    with ProjectEntitiesGenerators
    with ActivityGenerators
    with DatasetEntitiesGenerators {

  implicit val renkuBaseUrl: RenkuBaseUrl = Instances.renkuBaseUrl
  implicit val gitLabUrl:    GitLabUrl    = Instances.gitLabUrl
  implicit val gitLabApiUrl: GitLabApiUrl = Instances.gitLabApiUrl

  def invalidationTimes(min: InstantTinyType): Gen[InvalidationTime] = invalidationTimes(min.value)

  def invalidationTimes(min: Instant*): Gen[InvalidationTime] =
    timestamps(min = min.max, max = Instant.now()).toGeneratorOf(InvalidationTime)

  lazy val withGitLabId:    Gen[Option[GitLabId]] = personGitLabIds.toGeneratorOfSomes
  lazy val withoutGitLabId: Gen[Option[GitLabId]] = fixed(Option.empty[GitLabId])
  lazy val withEmail:       Gen[Option[Email]]    = personEmails.toGeneratorOfSomes
  lazy val withoutEmail:    Gen[Option[Email]]    = personEmails.toGeneratorOfNones

  implicit lazy val personEntities: Gen[Person] = personEntities()

  def personEntities(
      maybeGitLabIds: Gen[Option[GitLabId]] = personGitLabIds.toGeneratorOfOptions,
      maybeEmails:    Gen[Option[Email]] = personEmails.toGeneratorOfOptions
  ): Gen[Person] = for {
    name             <- personNames
    maybeEmail       <- maybeEmails
    maybeAffiliation <- personAffiliations.toGeneratorOfOptions
    maybeGitLabId    <- maybeGitLabIds
  } yield Person(name, maybeEmail, maybeAffiliation, maybeGitLabId)

  def replacePersonName(to: persons.Name): Person => Person = _.copy(name = to)
}

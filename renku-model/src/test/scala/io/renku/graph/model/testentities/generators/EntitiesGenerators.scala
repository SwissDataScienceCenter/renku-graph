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

package io.renku.graph.model.testentities
package generators

import cats.data.Kleisli
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.persons.{Email, GitLabId}
import org.scalacheck.Gen

object EntitiesGenerators extends EntitiesGenerators {
  type ProjectBasedGenFactory[A] = Kleisli[Gen, projects.DateCreated, A]

  type DatasetGenFactory[+P <: Dataset.Provenance] = projects.DateCreated => Gen[Dataset[P]]

  type ActivityGenFactory      = ProjectBasedGenFactory[Activity]
  type StepPlanGenFactory      = ProjectBasedGenFactory[StepPlan]
  type CompositePlanGenFactory = ProjectBasedGenFactory[CompositePlan]
  type PlanGenFactory          = ProjectBasedGenFactory[Plan]

  object ProjectBasedGenFactory {
    def pure[A](a:   A):      ProjectBasedGenFactory[A] = Kleisli.pure(a)
    def liftF[A](fa: Gen[A]): ProjectBasedGenFactory[A] = Kleisli.liftF(fa)
  }

  final implicit class ProjectBasedGenFactoryOps[A](self: ProjectBasedGenFactory[A]) {
    def generator: Gen[A] =
      self.run(projectCreatedDates().generateOne)

    def generateOne: A =
      generator.generateOne
  }
}

private object Instances {
  implicit val renkuUrl:     RenkuUrl     = RenkuTinyTypeGenerators.renkuUrls.generateOne
  implicit val gitLabUrl:    GitLabUrl    = gitLabUrls.generateOne
  implicit val gitLabApiUrl: GitLabApiUrl = gitLabUrl.apiV4
}

trait EntitiesGenerators
    extends RenkuProjectEntitiesGenerators
    with NonRenkuProjectEntitiesGenerators
    with ProjectEntitiesGenerators
    with ActivityGenerators
    with DatasetEntitiesGenerators {

  implicit val renkuUrl:     RenkuUrl     = Instances.renkuUrl
  implicit val gitLabUrl:    GitLabUrl    = Instances.gitLabUrl
  implicit val gitLabApiUrl: GitLabApiUrl = Instances.gitLabApiUrl

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
    maybeGitLabId    <- maybeGitLabIds
    maybeOrcidId     <- personOrcidIds.toGeneratorOfOptions
    maybeAffiliation <- personAffiliations.toGeneratorOfOptions
  } yield Person(name, maybeEmail, maybeGitLabId, maybeOrcidId, maybeAffiliation)

  def replacePersonName(to: persons.Name): Person => Person = _.copy(name = to)
}

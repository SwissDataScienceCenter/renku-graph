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

package io.renku.knowledgegraph.entities
package finder

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.GraphModelGenerators._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.knowledgegraph.entities.Endpoint.Criteria
import io.renku.knowledgegraph.entities.Endpoint.Criteria.Filters
import io.renku.rdfstore.{InMemoryJenaForSpec, RenkuDataset}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class WorkflowsEntitiesFinderSpec
    extends AnyWordSpec
    with should.Matchers
    with FinderSpecOps
    with InMemoryJenaForSpec
    with RenkuDataset
    with IOSpec {

  "findEntities - in case of a forks with workflows" should {

    "de-duplicate workflows when on forked projects" in new TestCase {
      val original ::~ fork = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .forkOnce()
      val plan :: Nil = fork.plans.toList

      upload(to = renkuDataset, original, fork)

      val results = finder
        .findEntities(Criteria(Filters(entityTypes = Set(Filters.EntityType.Workflow))))
        .unsafeRunSync()
        .results

      results should {
        be(List((plan -> original).to[model.Entity.Workflow])) or
          be(List((plan -> fork).to[model.Entity.Workflow]))
      }
    }
  }

  "findEntities - in case of a workflows on forks with different visibility" should {

    "favour workflows on public projects if exist" in new TestCase {

      val publicProject = renkuProjectEntities(visibilityPublic)
        .withActivities(activityEntities(planEntities()))
        .generateOne

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val original ::~ fork = {
        val original ::~ fork = publicProject.forkOnce()
        original -> fork.copy(visibility = visibilityNonPublic.generateOne, members = Set(member))
      }
      val plan :: Nil = publicProject.plans.toList

      upload(to = renkuDataset, original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Workflow)),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List((plan -> publicProject).to[model.Entity.Workflow])
    }

    "favour workflows on internal projects over private projects if exist" in new TestCase {

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val internalProject = renkuProjectEntities(fixed(projects.Visibility.Internal))
        .modify(replaceMembers(to = Set(member)))
        .withActivities(activityEntities(planEntities()))
        .generateOne

      val original ::~ fork = {
        val original ::~ fork = internalProject.forkOnce()
        original -> fork.copy(visibility = projects.Visibility.Private, members = Set(member))
      }
      val plan :: Nil = internalProject.plans.toList

      upload(to = renkuDataset, original, fork)

      finder
        .findEntities(
          Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Workflow)),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List((plan -> internalProject).to[model.Entity.Workflow])
    }

    "select workflows on private projects if there are no projects with broader visibility" in new TestCase {

      val member = personEntities(personGitLabIds.toGeneratorOfSomes).generateOne
      val privateProject = renkuProjectEntities(fixed(projects.Visibility.Private))
        .modify(replaceMembers(to = Set(member)))
        .withActivities(activityEntities(planEntities()))
        .generateOne
      val plan :: Nil = privateProject.plans.toList

      upload(to = renkuDataset, privateProject)

      finder
        .findEntities(
          Criteria(filters = Filters(entityTypes = Set(Filters.EntityType.Workflow)),
                   maybeUser = member.toAuthUser.some
          )
        )
        .unsafeRunSync()
        .results shouldBe List((plan -> privateProject).to[model.Entity.Workflow])
    }
  }
}

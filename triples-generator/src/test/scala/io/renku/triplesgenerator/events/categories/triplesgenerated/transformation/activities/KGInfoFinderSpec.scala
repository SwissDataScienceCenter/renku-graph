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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.activities

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.graph.model.testentities._
import io.renku.graph.model.{activities, entities}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestExecutionTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGInfoFinderSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "findActivityAuthor" should {

    "return activity author's resourceIds" in new TestCase {
      val project = projectEntities(anyVisibility)
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .to[entities.ProjectWithoutParent]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      loadToStore(project)

      kgInfoFinder.findActivityAuthor(activity.resourceId).unsafeRunSync() shouldBe
        Some(activity.author.resourceId)
    }

    "return no author if there's no Activity with the given id" in new TestCase {
      kgInfoFinder
        .findActivityAuthor(activities.ResourceId(httpUrls().generateOne))
        .unsafeRunSync() shouldBe None
    }
  }

  "findAssociationPersonAgent" should {

    "return activity association person agent resourceIds" in new TestCase {
      val project = projectEntities(anyVisibility)
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .generateOne
        .to[entities.ProjectWithoutParent]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      loadToStore(project)

      val maybeAgentResourceId = activity.association match {
        case assoc: entities.Association.WithPersonAgent => Some(assoc.agent.resourceId)
        case _ => fail("expected Person agent")
      }

      kgInfoFinder.findAssociationPersonAgent(activity.resourceId).unsafeRunSync() shouldBe maybeAgentResourceId
    }

    "return no agent if there's no Activity with the given id" in new TestCase {
      kgInfoFinder
        .findAssociationPersonAgent(activities.ResourceId(httpUrls().generateOne))
        .unsafeRunSync() shouldBe None
    }

    "return no agent if there's association with SoftwareAgent agent" in new TestCase {
      val project = projectEntities(anyVisibility)
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .to[entities.ProjectWithoutParent]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      loadToStore(project)

      kgInfoFinder
        .findAssociationPersonAgent(activity.resourceId)
        .unsafeRunSync() shouldBe None
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO] = TestLogger[IO]()
    private val timeRecorder = new SparqlQueryTimeRecorder(TestExecutionTimeRecorder[IO]())
    val kgInfoFinder         = new KGInfoFinderImpl(rdfStoreConfig, timeRecorder)
  }
}

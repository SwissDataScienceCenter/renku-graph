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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.activities

import cats.effect.IO
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.httpUrls
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model.{activities, entities}
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class KGInfoFinderSpec extends AnyWordSpec with IOSpec with InMemoryRdfStore with should.Matchers {

  "findActivityAuthors" should {

    "return activity author's resourceIds" in new TestCase {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      loadToStore(project)

      val person = personEntities.generateOne.to[entities.Person]
      loadToStore(person)
      insertTriple(activity.resourceId, "prov:wasAssociatedWith", person.resourceId.showAs[RdfResource])

      kgInfoFinder.findActivityAuthors(activity.resourceId).unsafeRunSync() shouldBe
        Set(activity.author.resourceId, person.resourceId)
    }

    "return no author if there's no Activity with the given id" in new TestCase {
      kgInfoFinder
        .findActivityAuthors(activities.ResourceId(httpUrls().generateOne))
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  "findAssociationPersonAgents" should {

    "return activity association person agent resourceIds" in new TestCase {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .generateOne
        .to[entities.RenkuProject]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      loadToStore(project)

      val person = personEntities.generateOne.to[entities.Person]
      val updatedAgentActivity = activity.copy(association = activity.association match {
        case assoc: entities.Association.WithPersonAgent => assoc.copy(agent = person)
        case _ => fail("Association.WithPersonAgent expected")
      })
      loadToStore(updatedAgentActivity)

      val originalAgent = activity.association match {
        case assoc: entities.Association.WithPersonAgent => assoc.agent
        case _ => fail("expected Person agent")
      }

      kgInfoFinder.findAssociationPersonAgents(activity.resourceId).unsafeRunSync() shouldBe
        Set(originalAgent.resourceId, person.resourceId)
    }

    "return no agent if there's no Activity with the given id" in new TestCase {
      kgInfoFinder
        .findAssociationPersonAgents(activities.ResourceId(httpUrls().generateOne))
        .unsafeRunSync() shouldBe Set.empty
    }

    "return no agent if there's association with SoftwareAgent agent" in new TestCase {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .generateOne
        .to[entities.RenkuProject]

      val activity = project.activities.headOption.getOrElse(fail("Activity expected"))

      loadToStore(project)

      kgInfoFinder
        .findAssociationPersonAgents(activity.resourceId)
        .unsafeRunSync() shouldBe Set.empty
    }
  }

  private trait TestCase {
    private implicit val logger:       TestLogger[IO]              = TestLogger[IO]()
    private implicit val timeRecorder: SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    val kgInfoFinder = new KGInfoFinderImpl[IO](renkuStoreConfig)
  }
}

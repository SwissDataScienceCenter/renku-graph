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

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations

import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import io.renku.graph.model.testentities._
import io.renku.graph.model._
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class MalformedActivityIdsSpec extends AnyWordSpec with should.Matchers with IOSpec with InMemoryRdfStore {

  "query" should {

    "find projects having at least one Activity with broken Resource Id" in {
      val project1 = {
        val p = renkuProjectEntities(anyVisibility)
          .withActivities(activityEntities(planEntities()))
          .withActivities(activityEntities(planEntities()))
          .generateOne
          .to[entities.RenkuProject.WithoutParent]
        val activity1 :: activity2 :: Nil = p.activities
        p.copy(activities = List(activity1, makeResourceIdBroken(activity2)))
      }
      val project2 = {
        val p = renkuProjectEntities(anyVisibility)
          .withActivities(activityEntities(planEntities()))
          .generateOne
          .to[entities.RenkuProject.WithoutParent]
        p.copy(activities = p.activities.map(makeResourceIdBroken))
      }

      loadToStore(
        project1,
        project2,
        anyRenkuProjectEntities
          .withActivities(activityEntities(planEntities()))
          .generateOne
          .to[entities.RenkuProject],
        anyRenkuProjectEntities.generateOne.to[entities.RenkuProject]
      )

      runQuery(MalformedActivityIds.query)
        .unsafeRunSync()
        .map(row => projects.Path(row("path")))
        .toSet shouldBe Set(project1.path, project2.path)
    }
  }

  private def makeResourceIdBroken(activity: entities.Activity): entities.Activity =
    activity.copy(resourceId = activities.ResourceId(activity.resourceId.value.replace("/activities/", "")))

  implicit val renkuBaseUrl: RenkuBaseUrl = RenkuBaseUrl(s"http://${nonEmptyStrings().generateOne}")
}

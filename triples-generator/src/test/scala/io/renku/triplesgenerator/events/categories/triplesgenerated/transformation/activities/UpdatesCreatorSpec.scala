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

package io.renku.triplesgenerator.events.categories.triplesgenerated.transformation.activities

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.userResourceIds
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.rdfstore.InMemoryRdfStore
import io.renku.testtools.IOSpec
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with InMemoryRdfStore
    with should.Matchers
    with ScalaCheckPropertyChecks {

  "queriesUnlinkingCreators" should {

    "prepare delete query for activity of which author exists in KG but not on the model" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      loadToStore(kgProject)

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))
      findAuthors(activity.resourceId) shouldBe Set(activity.author.resourceId)

      val newAuthor     = personEntities.generateOne.to[entities.Person]
      val modelActivity = activity.copy(author = newAuthor)

      UpdatesCreator
        .queriesUnlinkingAuthor(modelActivity, activity.author.resourceId.some)
        .runAll
        .unsafeRunSync()

      findAuthors(activity.resourceId) shouldBe Set.empty
    }

    "prepare no queries if there's no author in KG" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAuthor(activity, maybeKgAuthor = None) shouldBe Nil
    }

    "prepare no queries if there's no change in Activity author" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAuthor(activity, activity.author.resourceId.some) shouldBe Nil
    }
  }

  "queriesUnlinkingAgent" should {

    "prepare delete query for association a person agent which exists in KG but not on the model" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      loadToStore(kgProject)

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))
      findPersonAgents(activity.association.resourceId) shouldBe activity.association.maybePersonAgentResourceId.toSet

      val newAgent = personEntities.generateOne.to[entities.Person]
      val modelActivity = activity.association match {
        case assoc: entities.Association.WithPersonAgent => activity.copy(association = assoc.copy(agent = newAgent))
        case _ => fail("Expected Association.WithPersonAgent")
      }

      UpdatesCreator
        .queriesUnlinkingAgent(modelActivity, activity.association.maybePersonAgentResourceId)
        .runAll
        .unsafeRunSync()

      findPersonAgents(activity.association.resourceId) shouldBe Set.empty
    }

    "prepare no queries for association with SoftwareAgent" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator
        .queriesUnlinkingAgent(activity, maybeKgAgent = userResourceIds.generateOne.some) shouldBe Nil
    }

    "prepare no queries if there's no Person agent in KG" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAgent(activity, maybeKgAgent = None) shouldBe Nil
    }

    "prepare no queries if there's no change in association's person agent" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAgent(activity, activity.association.maybePersonAgentResourceId) shouldBe Nil
    }
  }

  private def findAuthors(resourceId: activities.ResourceId): Set[users.ResourceId] =
    runQuery(s"""|SELECT ?personId
                 |WHERE {
                 |  ${resourceId.showAs[RdfResource]} a prov:Activity;
                 |                                    prov:wasAssociatedWith ?personId.
                 |  ?personId a schema:Person.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => users.ResourceId.from(row("personId")))
      .sequence
      .fold(throw _, identity)
      .toSet

  private def findPersonAgents(resourceId: associations.ResourceId): Set[users.ResourceId] =
    runQuery(s"""|SELECT ?agentId
                 |WHERE {
                 |  ${resourceId.showAs[RdfResource]} a prov:Association;
                 |                                    prov:agent ?agentId.
                 |  ?agentId a schema:Person.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => users.ResourceId.from(row("agentId")))
      .sequence
      .fold(throw _, identity)
      .toSet

  private implicit class AssociationOps(association: entities.Association) {
    lazy val maybePersonAgentResourceId: Option[users.ResourceId] = association match {
      case assoc: entities.Association.WithPersonAgent => assoc.agent.resourceId.some
      case _ => None
    }
  }
}

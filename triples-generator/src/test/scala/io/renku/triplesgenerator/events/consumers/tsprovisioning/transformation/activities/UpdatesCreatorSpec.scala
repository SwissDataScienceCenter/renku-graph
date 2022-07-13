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

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.personResourceIds
import eu.timepit.refined.auto._
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import io.renku.testtools.IOSpec
import org.apache.jena.util.URIref
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with RenkuDataset
    with ScalaCheckPropertyChecks {

  "queriesUnlinkingAuthors" should {

    "prepare delete query if new activity has different author that it's set in the TS" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = renkuDataset, kgProject)

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))
      findAuthors(activity.resourceId).map(_.value) shouldBe Set(activity.author.resourceId)
        .map(id => URIref.encode(id.value))

      val newAuthor     = personEntities.generateOne.to[entities.Person]
      val modelActivity = activity.copy(author = newAuthor)

      UpdatesCreator
        .queriesUnlinkingAuthors(modelActivity, Set(activity.author.resourceId))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findAuthors(activity.resourceId) shouldBe Set.empty
    }

    "prepare delete query if there's more than one author for the activity in the TS" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = renkuDataset, kgProject)

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      val person = personEntities.generateOne.to[entities.Person]
      upload(to = renkuDataset, person)
      insert(to = renkuDataset, Triple.edge(activity.resourceId, prov / "wasAssociatedWith", person.resourceId))

      findAuthors(activity.resourceId).map(_.value) shouldBe Set(activity.author.resourceId, person.resourceId)
        .map(id => URIref.encode(id.value))

      UpdatesCreator
        .queriesUnlinkingAuthors(activity, Set(activity.author.resourceId, personResourceIds.generateOne))
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findAuthors(activity.resourceId) shouldBe Set.empty
    }

    "prepare no queries if there's no author in KG" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAuthors(activity, kgAuthors = Set.empty) shouldBe Nil
    }

    "prepare no queries if there's no change in Activity author" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAuthors(activity, Set(activity.author.resourceId)) shouldBe Nil
    }
  }

  "queriesUnlinkingAgents" should {

    "prepare delete query if the new Association's person agent is different from what's set in the TS" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = renkuDataset, kgProject)

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))
      findPersonAgents(activity.association.resourceId) shouldBe activity.association.maybePersonAgentResourceId.toSet

      val newAgent = personEntities.generateOne.to[entities.Person]
      val modelActivity = activity.association match {
        case assoc: entities.Association.WithPersonAgent => activity.copy(association = assoc.copy(agent = newAgent))
        case _ => fail("Expected Association.WithPersonAgent")
      }

      UpdatesCreator
        .queriesUnlinkingAgents(modelActivity, activity.association.maybePersonAgentResourceId.toSet)
        .runAll(on = renkuDataset)
        .unsafeRunSync()

      findPersonAgents(activity.association.resourceId) shouldBe Set.empty
    }

    "prepare delete query if there's more than one person agent for the Association set in the TS" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = renkuDataset, kgProject)

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))
      val person   = personEntities.generateOne.to[entities.Person]
      upload(to = renkuDataset, person)
      insert(to = renkuDataset, Triple.edge(activity.association.resourceId, prov / "agent", person.resourceId))

      findPersonAgents(activity.association.resourceId) shouldBe
        activity.association.maybePersonAgentResourceId.toSet + person.resourceId

      UpdatesCreator
        .queriesUnlinkingAgents(activity, activity.association.maybePersonAgentResourceId.toSet + person.resourceId)
        .runAll(on = renkuDataset)
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
        .queriesUnlinkingAgents(activity, kgAgents = Set(personResourceIds.generateOne)) shouldBe Nil
    }

    "prepare no queries if there's no Person agent in KG" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAgents(activity, kgAgents = Set.empty) shouldBe Nil
    }

    "prepare no queries if there's no change in association's person agent" in {
      val kgProject = anyRenkuProjectEntities
        .withActivities(activityEntities(planEntities()).modify(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = kgProject.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAgents(activity,
                                            activity.association.maybePersonAgentResourceId.toSet
      ) shouldBe Nil
    }
  }

  private def findAuthors(resourceId: activities.ResourceId): Set[persons.ResourceId] = runSelect(
    on = renkuDataset,
    SparqlQuery.of(
      "fetch activity creator",
      Prefixes of (prov -> "prov", schema -> "schema"),
      s"""|SELECT ?personId
          |WHERE {
          |  ${resourceId.showAs[RdfResource]} a prov:Activity;
          |                                    prov:wasAssociatedWith ?personId.
          |  ?personId a schema:Person.
          |}
          |""".stripMargin
    )
  ).unsafeRunSync()
    .map(row => persons.ResourceId.from(row("personId")))
    .sequence
    .fold(throw _, identity)
    .toSet

  private def findPersonAgents(resourceId: associations.ResourceId): Set[persons.ResourceId] = runSelect(
    on = renkuDataset,
    SparqlQuery.of(
      "fetch agent",
      Prefixes.of(prov -> "prov", schema -> "schema"),
      s"""|SELECT ?agentId
          |WHERE {
          |  ${resourceId.showAs[RdfResource]} a prov:Association;
          |                                    prov:agent ?agentId.
          |  ?agentId a schema:Person.
          |}
          |""".stripMargin
    )
  ).unsafeRunSync()
    .map(row => persons.ResourceId.from(row("agentId")))
    .sequence
    .fold(throw _, identity)
    .toSet

  private implicit class AssociationOps(association: entities.Association) {
    lazy val maybePersonAgentResourceId: Option[persons.ResourceId] = association match {
      case assoc: entities.Association.WithPersonAgent => assoc.agent.resourceId.some
      case _ => None
    }
  }
}

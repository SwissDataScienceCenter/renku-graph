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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.activities

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import GraphModelGenerators.personResourceIds
import eu.timepit.refined.auto._
import io.renku.graph.model.entities.{ActivityLens, AssociationLens, EntityFunctions}
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLDEncoder, NamedGraph}
import io.renku.testtools.IOSpec
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import io.renku.triplesstore.client.model.Quad
import org.apache.jena.util.URIref
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class UpdatesCreatorSpec
    extends AnyWordSpec
    with IOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    with ProjectsDataset
    with ScalaCheckPropertyChecks {

  "queriesUnlinkingAuthors" should {

    "prepare delete query if new activity has different author that it's set in the TS" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = projectsDataset, project)

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))
      findAuthors(project.resourceId, activity.resourceId).map(_.value) shouldBe Set(activity.author.resourceId)
        .map(id => URIref.encode(id.value))

      val modelActivity = activity.copy(author = personEntities.generateOne.to[entities.Person])

      UpdatesCreator
        .queriesUnlinkingAuthors(project.resourceId, modelActivity, Set(activity.author.resourceId))
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findAuthors(project.resourceId, activity.resourceId) shouldBe Set.empty
    }

    "prepare delete query if there's more than one author for the activity in the TS" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = projectsDataset, project)

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))

      val person = personEntities.generateOne.to[entities.Person]
      upload(
        to = projectsDataset, {
          implicit val enc: JsonLDEncoder[entities.Person] =
            EntityFunctions[entities.Person].encoder(GraphClass.Persons)
          NamedGraph.fromJsonLDsUnsafe(GraphClass.Persons.id, person.asJsonLD)
        }
      )
      insert(
        to = projectsDataset,
        Quad(GraphClass.Project.id(project.resourceId),
             activity.resourceId.asEntityId,
             prov / "wasAssociatedWith",
             person.resourceId.asEntityId
        )
      )

      findAuthors(project.resourceId, activity.resourceId).map(_.value) shouldBe Set(activity.author.resourceId,
                                                                                     person.resourceId
      ).map(id => URIref.encode(id.value))

      UpdatesCreator
        .queriesUnlinkingAuthors(project.resourceId,
                                 activity,
                                 Set(activity.author.resourceId, personResourceIds.generateOne)
        )
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findAuthors(project.resourceId, activity.resourceId) shouldBe Set.empty
    }

    "prepare no queries if there's no author in KG" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAuthors(project.resourceId, activity, kgAuthors = Set.empty) shouldBe Nil
    }

    "prepare no queries if there's no change in Activity author" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAuthors(project.resourceId, activity, Set(activity.author.resourceId)) shouldBe Nil
    }
  }

  "queriesUnlinkingAgents" should {

    "prepare delete query if the new Association's person agent is different from what's set in the TS" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()).map(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = projectsDataset, project)

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))
      findPersonAgents(project.resourceId,
                       activity.association.resourceId
      ) shouldBe activity.association.maybePersonAgentResourceId.toSet

      val newAgent = personEntities.generateOne.to[entities.Person]
      val modelActivity = ActivityLens.activityAssociationAgent.modify(
        _.requireRight("Expected Association.WithPersonAgent").as(newAgent)
      )(activity)

      UpdatesCreator
        .queriesUnlinkingAgents(project.resourceId,
                                modelActivity,
                                activity.association.maybePersonAgentResourceId.toSet
        )
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findPersonAgents(project.resourceId, activity.association.resourceId) shouldBe Set.empty
    }

    "prepare delete query if there's more than one person agent for the Association set in the TS" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()).map(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      upload(to = projectsDataset, project)

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))
      val person   = personEntities.generateOne.to[entities.Person]
      upload(
        to = projectsDataset, {
          implicit val enc: JsonLDEncoder[entities.Person] =
            EntityFunctions[entities.Person].encoder(GraphClass.Persons)
          NamedGraph.fromJsonLDsUnsafe(GraphClass.Persons.id, person.asJsonLD)
        }
      )
      insert(
        to = projectsDataset,
        Quad(GraphClass.Project.id(project.resourceId),
             activity.association.resourceId.asEntityId,
             prov / "agent",
             person.resourceId.asEntityId
        )
      )

      findPersonAgents(project.resourceId, activity.association.resourceId) shouldBe
        activity.association.maybePersonAgentResourceId.toSet + person.resourceId

      UpdatesCreator
        .queriesUnlinkingAgents(project.resourceId,
                                activity,
                                activity.association.maybePersonAgentResourceId.toSet + person.resourceId
        )
        .runAll(on = projectsDataset)
        .unsafeRunSync()

      findPersonAgents(project.resourceId, activity.association.resourceId) shouldBe Set.empty
    }

    "prepare no queries for association with SoftwareAgent" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator
        .queriesUnlinkingAgents(project.resourceId,
                                activity,
                                kgAgents = Set(personResourceIds.generateOne)
        ) shouldBe Nil
    }

    "prepare no queries if there's no Person agent in KG" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()).map(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAgents(project.resourceId, activity, kgAgents = Set.empty) shouldBe Nil
    }

    "prepare no queries if there's no change in association's person agent" in {
      val project = anyRenkuProjectEntities
        .withActivities(activityEntities(stepPlanEntities()).map(toAssociationPersonAgent))
        .map(_.to[entities.RenkuProject])
        .generateOne

      val activity = project.activities.headOption.getOrElse(fail("Expected activity"))

      UpdatesCreator.queriesUnlinkingAgents(project.resourceId,
                                            activity,
                                            activity.association.maybePersonAgentResourceId.toSet
      ) shouldBe Nil
    }
  }

  private def findAuthors(projectId: projects.ResourceId, resourceId: activities.ResourceId): Set[persons.ResourceId] =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "fetch activity creator",
        Prefixes of (prov -> "prov", schema -> "schema"),
        s"""|SELECT ?personId
            |FROM <${GraphClass.Project.id(projectId)}>
            |FROM <${GraphClass.Persons.id}> {
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

  private def findPersonAgents(projectId:  projects.ResourceId,
                               resourceId: associations.ResourceId
  ): Set[persons.ResourceId] = runSelect(
    on = projectsDataset,
    SparqlQuery.of(
      "fetch agent",
      Prefixes.of(prov -> "prov", schema -> "schema"),
      s"""|SELECT ?agentId
          |FROM <${GraphClass.Project.id(projectId)}>
          |FROM <${GraphClass.Persons.id}> {
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
    lazy val maybePersonAgentResourceId: Option[persons.ResourceId] =
      AssociationLens.associationAgent.get(association).toOption.map(_.resourceId)
  }

  private final implicit class EitherAssertions[A, B](eab: Either[A, B]) {
    def requireRight(message: String): Either[A, B] =
      eab.filterOrElse(_ => true, fail(message))
  }
}

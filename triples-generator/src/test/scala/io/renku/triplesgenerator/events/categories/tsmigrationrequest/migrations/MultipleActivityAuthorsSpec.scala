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

import cats.effect.IO
import cats.syntax.all._
import io.renku.config.ServiceVersion
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.graph.model.views.RdfResource
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.metrics.MetricsRegistry
import io.renku.rdfstore.{InMemoryRdfStore, SparqlQueryTimeRecorder}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import tooling._

class MultipleActivityAuthorsSpec
    extends AnyWordSpec
    with should.Matchers
    with IOSpec
    with InMemoryRdfStore
    with MockFactory {

  "run" should {

    "find Activity records with multiple prov:wasAssociatedWith a Person object and remove the additional ones " +
      "- case when one of the authors doesn't have GitLabId" in new TestCase {
        satisfyMocks

        val project = anyRenkuProjectEntities
          .withActivities(
            activityEntities(planEntities())
              .modify(toAssociationPersonAgent(personEntities(withGitLabId, withEmail).generateOne))
          )
          .map(_.to[entities.RenkuProject])
          .generateOne

        loadToStore(project)

        val activity = project.activities.headOption.getOrElse(fail("Expected activity"))
        val author2  = personEntities(withoutGitLabId, withEmail).generateOne.to[entities.Person]
        loadToStore(author2)
        insertTriple(activity.resourceId, "prov:wasAssociatedWith", author2.resourceId.showAs[RdfResource])

        findActivityAuthors(activity.resourceId) shouldBe Set(activity.author.resourceId, author2.resourceId)

        migration.run().value.unsafeRunSync() shouldBe ().asRight

        findActivityAuthors(activity.resourceId) shouldBe Set(activity.author.resourceId)
      }

    "find Activity records with multiple prov:wasAssociatedWith a Person object and remove the additional ones " +
      "- case when all authors without GitLabId" in new TestCase {
        satisfyMocks

        val project = anyRenkuProjectEntities
          .withActivities(
            activityEntities(planEntities())
              .modify(toAssociationPersonAgent(personEntities(withoutGitLabId, withEmail).generateOne))
          )
          .map(_.to[entities.RenkuProject])
          .generateOne

        loadToStore(project)

        val activity = project.activities.headOption.getOrElse(fail("Expected activity"))
        val author2  = personEntities(withoutGitLabId, withEmail).generateOne.to[entities.Person]
        loadToStore(author2)
        insertTriple(activity.resourceId, "prov:wasAssociatedWith", author2.resourceId.showAs[RdfResource])

        findActivityAuthors(activity.resourceId) shouldBe Set(activity.author.resourceId, author2.resourceId)

        migration.run().value.unsafeRunSync() shouldBe ().asRight

        findActivityAuthors(activity.resourceId) should contain oneElementOf Set(activity.author.resourceId,
                                                                                 author2.resourceId
        )
      }

    "find Activity records with multiple prov:wasAssociatedWith a Person object and remove the additional ones " +
      "- case when all authors have GitLabId" in new TestCase {
        satisfyMocks

        val project = anyRenkuProjectEntities
          .withActivities(
            activityEntities(planEntities())
              .modify(toAssociationPersonAgent(personEntities(withGitLabId, withEmail).generateOne))
          )
          .map(_.to[entities.RenkuProject])
          .generateOne

        loadToStore(project)

        val activity = project.activities.headOption.getOrElse(fail("Expected activity"))
        val author2  = personEntities(withGitLabId, withEmail).generateOne.to[entities.Person]
        loadToStore(author2)
        insertTriple(activity.resourceId, "prov:wasAssociatedWith", author2.resourceId.showAs[RdfResource])

        findActivityAuthors(activity.resourceId) shouldBe Set(activity.author.resourceId, author2.resourceId)

        migration.run().value.unsafeRunSync() shouldBe ().asRight

        findActivityAuthors(activity.resourceId) should contain oneElementOf Set(activity.author.resourceId,
                                                                                 author2.resourceId
        )
      }
  }

  "apply" should {
    "return an RegisteredMigration" in new TestCase {
      migration.getClass.getSuperclass shouldBe classOf[RegisteredMigration[IO]]
    }
  }

  private trait TestCase {
    implicit val logger:          TestLogger[IO]              = TestLogger[IO]()
    implicit val timeRecorder:    SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO]
    implicit val metricsRegistry: MetricsRegistry[IO]         = new MetricsRegistry.DisabledMetricsRegistry[IO]()
    val executionRegister = mock[MigrationExecutionRegister[IO]]
    val recordsFinder     = RecordsFinder[IO](rdfStoreConfig)
    val updateRunner      = UpdateQueryRunner[IO](rdfStoreConfig)
    val migration         = new MultipleActivityAuthors[IO](executionRegister, recordsFinder, updateRunner)

    lazy val satisfyMocks = {
      (executionRegister.findExecution _).expects(migration.name).returning(Option.empty[ServiceVersion].pure[IO])
      (executionRegister.registerExecution _).expects(migration.name).returning(().pure[IO])
    }
  }

  private def findActivityAuthors(resourceId: activities.ResourceId): Set[persons.ResourceId] =
    runQuery(s"""|SELECT ?personId
                 |WHERE {
                 |  ${resourceId.showAs[RdfResource]} a prov:Activity;
                 |                                    prov:wasAssociatedWith ?personId.
                 |  ?personId a schema:Person.
                 |}
                 |""".stripMargin)
      .unsafeRunSync()
      .map(row => persons.ResourceId(row("personId")))
      .toSet
}

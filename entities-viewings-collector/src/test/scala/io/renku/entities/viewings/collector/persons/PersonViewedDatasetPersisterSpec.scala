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

package io.renku.entities.viewings.collector.persons

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.entities.viewings.collector
import io.renku.generators.Generators.{fixed, timestamps, timestampsNotInTheFuture}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model._
import io.renku.graph.model.testentities._
import io.renku.graph.model.Schemas.renku
import io.renku.interpreters.TestLogger
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.IOSpec
import io.renku.triplesgenerator.api.events.Generators.userIds
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore._
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import org.scalatest.OptionValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant

class PersonViewedDatasetPersisterSpec
    extends AnyWordSpec
    with should.Matchers
    with OptionValues
    with IOSpec
    with InMemoryJenaForSpec
    with ProjectsDataset {

  "persist" should {

    "insert the given GLUserViewedDataset to the TS if it doesn't exist yet " +
      "case with a user identified with GitLab id" in new TestCase {

        val userId             = UserId(personGitLabIds.generateOne)
        val dataset -> project = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project)

        val dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
        val event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
        )
      }

    "insert the given GLUserViewedDataset to the TS if it doesn't exist yet " +
      "case with a user identified with email" in new TestCase {

        val userId             = UserId(personEmails.generateOne)
        val dataset -> project = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project)

        val dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
        val event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
        )
      }

    "update the date for the user and ds from the GLUserViewedDataset " +
      "if an event for the ds already exists in the TS " +
      "and the date from the new event is newer than this in the TS" in new TestCase {

        val userId             = userIds.generateOne
        val dataset -> project = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project)

        val dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
        val event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

        persister.persist(event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
        )

        val newDate = timestampsNotInTheFuture(butYoungerThan = event.date.value).generateAs(datasets.DateViewed)

        persister.persist(event.copy(date = newDate)).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, newDate))
      }

    "do nothing if the event date is older than the date in the TS" in new TestCase {

      val userId             = userIds.generateOne
      val dataset -> project = generateProjectWithCreator(userId)

      upload(to = projectsDataset, project)

      val dateViewed = datasetViewedDates(dataset.provenance.date.instant).generateOne
      val event      = GLUserViewedDataset(userId, toCollectorDataset(dataset), dateViewed)

      persister.persist(event).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(
        ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed)
      )

      val newDate = timestamps(max = event.date.value.minusSeconds(1)).generateAs(datasets.DateViewed)

      persister.persist(event.copy(date = newDate)).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set(ViewingRecord(project.maybeCreator.value.resourceId, dataset.resourceId, dateViewed))
    }

    "update the date for the user and project from the GLUserViewedProject " +
      "and leave other user viewings if they exist" in new TestCase {

        val userId               = userIds.generateOne
        val dataset1 -> project1 = generateProjectWithCreator(userId)
        val dataset2 -> project2 = generateProjectWithCreator(userId)

        upload(to = projectsDataset, project1, project2)

        val dataset1DateViewed = datasetViewedDates(dataset1.provenance.date.instant).generateOne
        val dataset1Event =
          collector.persons.GLUserViewedDataset(userId, toCollectorDataset(dataset1), dataset1DateViewed)
        persister.persist(dataset1Event).unsafeRunSync() shouldBe ()

        val dataset2DateViewed = datasetViewedDates(dataset2.provenance.date.instant).generateOne
        val dataset2Event =
          collector.persons.GLUserViewedDataset(userId, toCollectorDataset(dataset2), dataset2DateViewed)
        persister.persist(dataset2Event).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project1.maybeCreator.value.resourceId, dataset1.resourceId, dataset1DateViewed),
          ViewingRecord(project2.maybeCreator.value.resourceId, dataset2.resourceId, dataset2DateViewed)
        )

        val newDate =
          timestampsNotInTheFuture(butYoungerThan = dataset1Event.date.value).generateAs(datasets.DateViewed)

        persister.persist(dataset1Event.copy(date = newDate)).unsafeRunSync() shouldBe ()

        findAllViewings shouldBe Set(
          ViewingRecord(project1.maybeCreator.value.resourceId, dataset1.resourceId, newDate),
          ViewingRecord(project2.maybeCreator.value.resourceId, dataset2.resourceId, dataset2DateViewed)
        )
      }

    "do nothing if the given event is for a non-existing user" in new TestCase {

      val dataset -> _ = generateProjectWithCreator(userIds.generateOne)

      val event = collector.persons.GLUserViewedDataset(userIds.generateOne,
                                                        toCollectorDataset(dataset),
                                                        datasetViewedDates(dataset.provenance.date.instant).generateOne
      )

      persister.persist(event).unsafeRunSync() shouldBe ()

      findAllViewings shouldBe Set.empty
    }
  }

  private trait TestCase {
    private implicit val logger: TestLogger[IO]              = TestLogger[IO]()
    private implicit val sqtr:   SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder[IO].unsafeRunSync()
    private val tsClient = TSClient[IO](projectsDSConnectionInfo)
    val persister        = new PersonViewedDatasetPersisterImpl[IO](tsClient, PersonFinder(tsClient))
  }

  private def generateProjectWithCreator(userId: UserId) = {

    val creator = userId
      .fold(
        glId => personEntities(maybeGitLabIds = fixed(glId.some)).map(removeOrcidId),
        email => personEntities(withoutGitLabId, maybeEmails = fixed(email.some)).map(removeOrcidId)
      )
      .generateSome

    anyRenkuProjectEntities
      .map(replaceProjectCreator(creator))
      .addDataset(datasetEntities(provenanceInternal))
      .generateOne
      .bimap(
        _.to[entities.Dataset[entities.Dataset.Provenance.Internal]],
        _.to[entities.Project]
      )
  }

  private def findAllViewings =
    runSelect(
      on = projectsDataset,
      SparqlQuery.of(
        "test find user project viewings",
        Prefixes of renku -> "renku",
        sparql"""|SELECT ?id ?datasetId ?date
                 |FROM ${GraphClass.PersonViewings.id} {
                 |  ?id renku:viewedDataset ?viewingId.
                 |  ?viewingId renku:dataset ?datasetId;
                 |             renku:dateViewed ?date.
                 |}
                 |""".stripMargin
      )
    ).unsafeRunSync()
      .map(row =>
        ViewingRecord(persons.ResourceId(row("id")),
                      datasets.ResourceId(row("datasetId")),
                      datasets.DateViewed(Instant.parse(row("date")))
        )
      )
      .toSet

  private case class ViewingRecord(userId:    persons.ResourceId,
                                   datasetId: datasets.ResourceId,
                                   date:      datasets.DateViewed
  )

  private def toCollectorDataset(ds: entities.Dataset[entities.Dataset.Provenance]) =
    collector.persons.Dataset(ds.resourceId, ds.identification.identifier)
}
package io.renku.eventlog.subscriptions.membersync

import ch.datascience.db.SqlQuery
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.metrics.TestLabeledHistogram
import eu.timepit.refined.auto._
import io.renku.eventlog.DbEventLogGenerators._
import io.renku.eventlog.subscriptions.SubscriptionCategory.LastSyncedDate
import io.renku.eventlog.subscriptions.SubscriptionDataProvisioning
import io.renku.eventlog.{EventDate, InMemoryEventLogDbSpec}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Duration, Instant}

class MemberSyncEventFinderSpec
    extends AnyWordSpec
    with InMemoryEventLogDbSpec
    with SubscriptionDataProvisioning
    with MockFactory
    with should.Matchers {

  "popEvent" should {

    "return the event for the project with the latest event date " +
      "when the subscription_category_sync_times table is empty" in new TestCase {
        finder.popEvent().unsafeRunSync() shouldBe None

        val projectPath0 = projectPaths.generateOne
        val eventDate0   = eventDates.generateOne
        upsertProject(compoundEventIds.generateOne, projectPath0, eventDate0)
        val projectPath1 = projectPaths.generateOne
        val eventDate1   = eventDates.generateOne

        upsertProject(compoundEventIds.generateOne, projectPath1, eventDate1)
        val projectPathsByDateIncreasing =
          List((projectPath0, eventDate0), (projectPath1, eventDate1)).sortBy(_._2).map(_._1)
        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPathsByDateIncreasing.head))
        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPathsByDateIncreasing.tail.head))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return projects with a latest event date less than an hour ago" +
      "and a last sync time more than a minute ago" +
      "AND not projects with a latest event date less than an hour ago" +
      "and a last sync time less than a minute ago" in new TestCase {
        val compoundId0  = compoundEventIds.generateOne
        val projectPath0 = projectPaths.generateOne
        val eventDate0   = EventDate(generateInstant(lessThanAgo = Duration.ofMinutes(59)))
        val lastSynced0  = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofSeconds(61)))
        upsertProject(compoundId0, projectPath0, eventDate0)
        upsertLastSynced(compoundId0.projectId, SubscriptionCategory.name, lastSynced0)

        val compoundId1  = compoundEventIds.generateOne
        val projectPath1 = projectPaths.generateOne
        val eventDate1   = EventDate(generateInstant(lessThanAgo = Duration.ofMinutes(59)))
        val lastSynced1  = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofSeconds(59)))
        upsertProject(compoundId1, projectPath1, eventDate1)
        upsertLastSynced(compoundId1.projectId, SubscriptionCategory.name, lastSynced1)

        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPath0))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return projects with a latest event date less than an day ago" +
      "and a last sync time more than a hour ago" +
      "AND not projects with a latest event date less than an day ago" +
      "and a last sync time less than a hour ago" in new TestCase {
        val compoundId0  = compoundEventIds.generateOne
        val projectPath0 = projectPaths.generateOne
        val eventDate0   = EventDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
        val lastSynced0  = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofMinutes(61)))
        upsertProject(compoundId0, projectPath0, eventDate0)
        upsertLastSynced(compoundId0.projectId, SubscriptionCategory.name, lastSynced0)

        val compoundId1  = compoundEventIds.generateOne
        val projectPath1 = projectPaths.generateOne
        val eventDate1   = EventDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
        val lastSynced1  = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofMinutes(59)))
        upsertProject(compoundId1, projectPath1, eventDate1)
        upsertLastSynced(compoundId1.projectId, SubscriptionCategory.name, lastSynced1)

        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPath0))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

    "return projects with a latest event date more than a day ago" +
      "and a last sync time more than a day ago" +
      "AND not projects with a latest event date more than a day ago" +
      "and a last sync time less than a day ago" in new TestCase {
        val compoundId0  = compoundEventIds.generateOne
        val projectPath0 = projectPaths.generateOne
        val eventDate0   = EventDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
        val lastSynced0  = LastSyncedDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
        upsertProject(compoundId0, projectPath0, eventDate0)
        upsertLastSynced(compoundId0.projectId, SubscriptionCategory.name, lastSynced0)

        val compoundId1  = compoundEventIds.generateOne
        val projectPath1 = projectPaths.generateOne
        val eventDate1   = EventDate(generateInstant(moreThanAgo = Duration.ofHours(25)))
        val lastSynced1  = LastSyncedDate(generateInstant(lessThanAgo = Duration.ofHours(23)))
        upsertProject(compoundId1, projectPath1, eventDate1)
        upsertLastSynced(compoundId1.projectId, SubscriptionCategory.name, lastSynced1)

        finder.popEvent().unsafeRunSync() shouldBe Some(MemberSyncEvent(projectPath0))
        finder.popEvent().unsafeRunSync() shouldBe None
      }

  }

  private trait TestCase {

    val queriesExecTimes = TestLabeledHistogram[SqlQuery.Name]("query_id")

    val finder = new MemberSyncEventFinderImpl(transactor, queriesExecTimes)

    def generateInstant(lessThanAgo: Duration = Duration.ofDays(999999), moreThanAgo: Duration = Duration.ZERO) =
      timestamps(min = Instant.now.minus(lessThanAgo), max = Instant.now.minus(moreThanAgo)).generateOne
  }
}

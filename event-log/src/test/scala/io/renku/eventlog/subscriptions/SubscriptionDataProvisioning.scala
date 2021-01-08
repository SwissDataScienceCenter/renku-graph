package io.renku.eventlog.subscriptions

import ch.datascience.graph.model.projects
import io.renku.eventlog.subscriptions.SubscriptionCategory.{CategoryName, LastSyncedDate}
import io.renku.eventlog.{EventLogDataProvisioning, InMemoryEventLogDb}
import doobie.implicits._

trait SubscriptionDataProvisioning extends EventLogDataProvisioning with SubscriptionTypeSerializers {
  self: InMemoryEventLogDb =>

  protected def upsertLastSynced(projectId: projects.Id, categoryName: CategoryName, lastSynced: LastSyncedDate): Unit =
    execute {
      sql"""|INSERT INTO
            |subscription_category_sync_times (project_id, category_name, last_synced)
            |VALUES ($projectId, $categoryName, $lastSynced)
            |ON CONFLICT (project_id, category_name)
            |DO UPDATE SET  last_synced = excluded.last_synced 
      """.stripMargin.update.run.map(_ => ())
    }
}

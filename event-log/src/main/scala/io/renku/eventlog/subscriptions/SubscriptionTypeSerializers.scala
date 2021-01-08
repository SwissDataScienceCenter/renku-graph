package io.renku.eventlog.subscriptions
import doobie.util.{Get, Put}
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.subscriptions.SubscriptionCategory.{CategoryName, LastSyncedDate}

import java.time.Instant
trait SubscriptionTypeSerializers extends TypeSerializers {

  implicit val lastSyncedDateGet: Get[LastSyncedDate] = Get[Instant].tmap(LastSyncedDate.apply)
  implicit val lastSyncedDatePut: Put[LastSyncedDate] = Put[Instant].contramap(_.value)

  implicit val categoryNameGet: Get[CategoryName] = Get[String].tmap(CategoryName.apply)
  implicit val categoryNamePut: Put[CategoryName] = Put[String].contramap(_.value)
}

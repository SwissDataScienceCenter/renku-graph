package io.renku.lock

import cats.effect._
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import io.renku.db.PostgresContainer
import org.scalatest.Suite
import skunk.Session
import natchez.Trace.Implicits.noop
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait PostgresLockTest extends TestContainerForAll { self: Suite =>

  override val containerDef = PostgreSQLContainer.Def(
    dockerImageName = PostgresContainer.imageName,
    databaseName = "locktest",
    username = "pg",
    password = "pg"
  )

  def session(c: Containers): Resource[IO, Session[IO]] =
    Session.single[IO](
      host = c.host,
      port = c.underlyingUnsafeContainer.getFirstMappedPort,
      user = c.username,
      database = c.databaseName,
      password = Some(c.password)
    )

  def exclusiveLock(cnt: Containers, interval: FiniteDuration = 100.millis)(implicit L: Logger[IO]) =
    session(cnt).map(PostgresLock.exclusive[IO, String](_, interval))

  def sharedLock(cnt: Containers, interval: FiniteDuration = 100.millis)(implicit L: Logger[IO]) =
    session(cnt).map(PostgresLock.shared[IO, String](_, interval))
}

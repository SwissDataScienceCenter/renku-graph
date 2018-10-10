package persistence

import java.time.Instant

import play.api.db.slick.HasDatabaseConfig
import slick.jdbc.JdbcProfile

trait ImplicitsComponent {
  this: HasDatabaseConfig[JdbcProfile] =>

  import profile.api._

  implicit val customTimestampColumnType: BaseColumnType[Instant] =
    MappedColumnType.base[Instant, Long]( _.toEpochMilli, Instant.ofEpochMilli )
}

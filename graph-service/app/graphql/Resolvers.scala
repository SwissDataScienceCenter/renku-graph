package graphql

import models.{ Activity, Entity }
import persistence.DatabaseAccessLayer
import sangria.execution.deferred._

object Resolvers {
  lazy val activities: Fetcher[DatabaseAccessLayer, Activity, Activity, String] =
    Fetcher( ( ctx: DatabaseAccessLayer, ids: Seq[String] ) =>
      ctx.highLevel.activities( ids ) )( HasId( _.id ) )

  lazy val entities: Fetcher[DatabaseAccessLayer, Entity, Entity, String] =
    Fetcher( ( ctx: DatabaseAccessLayer, ids: Seq[String] ) =>
      ctx.highLevel.entities( ids ) )( HasId( _.id ) )

  lazy val resolver: DeferredResolver[DatabaseAccessLayer] =
    DeferredResolver.fetchers( activities, entities )
}

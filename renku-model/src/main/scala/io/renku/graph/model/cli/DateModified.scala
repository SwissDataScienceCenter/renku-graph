package io.renku.graph.model.cli

import io.renku.graph.model.views.TinyTypeJsonLDOps
import io.renku.tinytypes.constraints.InstantNotInTheFuture
import io.renku.tinytypes.{InstantTinyType, TinyTypeFactory}

import java.time.Instant

final class DateModified private (val value: Instant) extends InstantTinyType

object DateModified
    extends TinyTypeFactory[DateModified](new DateModified(_))
    with InstantNotInTheFuture[DateModified]
    with TinyTypeJsonLDOps[DateModified]

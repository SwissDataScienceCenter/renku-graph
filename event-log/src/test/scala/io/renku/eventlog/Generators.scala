package io.renku.eventlog

import ch.datascience.generators.Generators.jsons
import org.scalacheck.Gen

object Generators {

  implicit val eventPayloads: Gen[EventPayload] = for {
    content <- jsons
  } yield EventPayload(content.noSpaces)
}

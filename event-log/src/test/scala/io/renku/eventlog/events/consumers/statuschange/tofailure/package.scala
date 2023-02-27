package io.renku.eventlog.events.consumers
package statuschange

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.EventContentGenerators._
import io.renku.graph.model.EventsGenerators._
import io.renku.graph.model.GraphModelGenerators.projectPaths
import org.scalacheck.Gen

import java.time.Duration

package object tofailure {

  lazy val toFailureEvents = for {
    eventId        <- compoundEventIds
    projectPath    <- projectPaths
    message        <- eventMessages
    executionDelay <- executionDelays.toGeneratorOfOptions
    event <- Gen.oneOf(
               ToFailure(eventId,
                         projectPath,
                         message,
                         GeneratingTriples,
                         GenerationRecoverableFailure,
                         executionDelay
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         GeneratingTriples,
                         GenerationNonRecoverableFailure,
                         maybeExecutionDelay = None
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         TransformingTriples,
                         TransformationRecoverableFailure,
                         executionDelay
               ),
               ToFailure(eventId,
                         projectPath,
                         message,
                         TransformingTriples,
                         TransformationNonRecoverableFailure,
                         maybeExecutionDelay = None
               )
             )
  } yield event

  private lazy val executionDelays: Gen[Duration] = Gen.choose(0L, 10L).map(Duration.ofSeconds)
}

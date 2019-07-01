package ch.datascience.interpreters

import cats.effect.IO
import ch.datascience.config.sentry.SentryInitializer

abstract class IOSentryInitializer extends SentryInitializer[IO]

package io.renku.triplesgenerator.events.categories.tsmigrationrequest.migrations.tooling

import cats.MonadThrow
import cats.syntax.all._
import io.renku.http.client.RestClientError.{ClientException, ConnectivityException, UnexpectedResponseException}
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError
import io.renku.triplesgenerator.events.categories.ProcessingRecoverableError.LogWorthyRecoverableError
import org.http4s.Status.{Forbidden, InternalServerError, Unauthorized}

private[migrations] object RecoverableErrorsRecovery extends RecoverableErrorsRecovery

private[migrations] trait RecoverableErrorsRecovery {

  type RecoveryStrategy[F[_], OUT] = PartialFunction[Throwable, F[Either[ProcessingRecoverableError, OUT]]]

  def maybeRecoverableError[F[_]: MonadThrow, OUT]: RecoveryStrategy[F, OUT] = {
    case exception @ (_: ConnectivityException | _: ClientException) =>
      LogWorthyRecoverableError(exception.getMessage, exception.getCause)
        .asLeft[OUT]
        .leftWiden[ProcessingRecoverableError]
        .pure[F]
    case exception @ UnexpectedResponseException(Unauthorized | Forbidden | InternalServerError, _) =>
      LogWorthyRecoverableError(exception.getMessage, exception.getCause)
        .asLeft[OUT]
        .leftWiden[ProcessingRecoverableError]
        .pure[F]
    case exception @ UnexpectedResponseException(_, _) =>
      LogWorthyRecoverableError(exception.getMessage, exception.getCause)
        .asLeft[OUT]
        .leftWiden[ProcessingRecoverableError]
        .pure[F]
  }
}

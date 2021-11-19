package io.renku.eventlog.events.categories.statuschange.projectCleaner

import cats.effect.Async
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.webhookservice.WebhookServiceUrl
import io.renku.http.client.RestClient
import org.http4s.Method.DELETE
import org.http4s.Status.{NotFound, Ok}
import org.typelevel.log4cats.Logger

private trait ProjectWebhookAndTokenRemover[F[_]] {
  def removeWebhookAndToken(project: Project): F[Unit]
}

private object ProjectWebhookAndTokenRemover {
  def apply[F[_]: Async: Logger](): F[ProjectWebhookAndTokenRemover[F]] = for {
    webhookUrl <- WebhookServiceUrl()
  } yield new ProjectWebhookAndTokenRemoverImpl[F](webhookUrl)
}

private class ProjectWebhookAndTokenRemoverImpl[F[_]: Async: Logger](webhookUrl: WebhookServiceUrl)
    extends RestClient[F, ProjectWebhookAndTokenRemover[F]](Throttler.noThrottling[F])
    with ProjectWebhookAndTokenRemover[F] {
  private lazy val mapResponse: ResponseMapping[Unit] = {
    case (Ok | NotFound, _, _) => ().pure[F]
    case (status, _, _)        => new Exception(s"Removing webhook failed with status: $status").raiseError[F, Unit]
  }
  override def removeWebhookAndToken(project: Project): F[Unit] = for {
    _ <- removeProjectWebhook(project)
  } yield ()

  private def removeProjectWebhook(project: Project): F[Unit] = for {
    validatedUrl <- validateUri(s"$webhookUrl/projects/${project.id}/webhooks")
    _            <- send(request(DELETE, validatedUrl))(mapResponse)
  } yield ()

  private def removeProjectTokens(project: Project): F[Unit] = ???

}

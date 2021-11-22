package io.renku.eventlog.events.categories.statuschange.projectCleaner

import cats.effect.Async
import cats.syntax.all._
import io.renku.control.Throttler
import io.renku.events.consumers.Project
import io.renku.graph.tokenrepository.TokenRepositoryUrl
import io.renku.graph.webhookservice.WebhookServiceUrl
import io.renku.http.client.RestClient
import org.http4s.Method.DELETE
import org.http4s.Status.{NoContent, NotFound, Ok}
import org.typelevel.log4cats.Logger

private trait ProjectWebhookAndTokenRemover[F[_]] {
  def removeWebhookAndToken(project: Project): F[Unit]
}

private object ProjectWebhookAndTokenRemover {
  def apply[F[_]: Async: Logger](): F[ProjectWebhookAndTokenRemover[F]] = for {
    webhookUrl         <- WebhookServiceUrl()
    tokenRepositoryUrl <- TokenRepositoryUrl()
  } yield new ProjectWebhookAndTokenRemoverImpl[F](webhookUrl, tokenRepositoryUrl)
}

private class ProjectWebhookAndTokenRemoverImpl[F[_]: Async: Logger](webhookUrl: WebhookServiceUrl,
                                                                     tokenRepositoryUrl: TokenRepositoryUrl
) extends RestClient[F, ProjectWebhookAndTokenRemover[F]](Throttler.noThrottling[F])
    with ProjectWebhookAndTokenRemover[F] {
  private def mapWebhookResponse(project: Project): ResponseMapping[Unit] = {
    case (Ok | NotFound, _, _) => ().pure[F]
    case (status, _, _) =>
      new Exception(s"Removing project webhook failed with status: $status for project: ${project.show}")
        .raiseError[F, Unit]
  }
  private def mapTokenRepoResponse(project: Project): ResponseMapping[Unit] = {
    case (NoContent | NotFound, _, _) => ().pure[F]
    case (status, _, _) =>
      new Exception(s"Removing project token failed with status: $status for project: ${project.show}")
        .raiseError[F, Unit]
  }
  override def removeWebhookAndToken(project: Project): F[Unit] = for {
    _ <- removeProjectWebhook(project)
    _ <- removeProjectTokens(project)
  } yield ()

  private def removeProjectWebhook(project: Project): F[Unit] = for {
    validatedUrl <- validateUri(s"$webhookUrl/projects/${project.id}/webhooks")
    _            <- send(request(DELETE, validatedUrl))(mapWebhookResponse(project))
  } yield ()

  private def removeProjectTokens(project: Project): F[Unit] = for {
    validatedUrl <- validateUri(s"$tokenRepositoryUrl/projects/${project.id}/tokens")
    _            <- send(request(DELETE, validatedUrl))(mapTokenRepoResponse(project))
  } yield ()

}

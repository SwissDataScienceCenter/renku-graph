package io.renku.tokenrepository.repository.refresh

import io.renku.graph.model.projects
import io.renku.http.client.AccessToken

private trait ExpiredTokensFinder[F[_]] {
  def findExpiredTokens(projectId: projects.Id, accessToken: AccessToken): F[List[TokenId]]
}

private object ExpiredTokensFinder {}

private class ExpiredTokensFinderImpl[F[_]] extends ExpiredTokensFinder[F] {

  override def findExpiredTokens(projectId: projects.Id, accessToken: AccessToken): F[List[TokenId]] = ???
}

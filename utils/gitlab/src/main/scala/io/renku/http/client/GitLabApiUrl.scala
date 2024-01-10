package io.renku.http.client

import cats.syntax.all._
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}

final class GitLabUrl private (val value: String) extends AnyVal with UrlTinyType {
  def apiV4: GitLabApiUrl = GitLabApiUrl(this)
}
object GitLabUrl extends TinyTypeFactory[GitLabUrl](new GitLabUrl(_)) with Url[GitLabUrl] with UrlOps[GitLabUrl] {
  override val transform: String => Either[Throwable, String] = {
    case v if v.endsWith("/") => v.substring(0, v.length - 1).asRight
    case v                    => v.asRight
  }
}

final class GitLabApiUrl private (val value: String) extends AnyVal with UrlTinyType
object GitLabApiUrl
    extends TinyTypeFactory[GitLabApiUrl](new GitLabApiUrl(_))
    with Url[GitLabApiUrl]
    with UrlOps[GitLabApiUrl] {
  def apply(gitLabUrl: GitLabUrl): GitLabApiUrl = new GitLabApiUrl((gitLabUrl / "api" / "v4").value)
}

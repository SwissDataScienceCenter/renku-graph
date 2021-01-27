package ch.datascience.http.server

import ch.datascience.http.client.AccessToken
import ch.datascience.http.client.AccessToken.{OAuthAccessToken, PersonalAccessToken}
import org.http4s.AuthScheme.Bearer
import org.http4s.Credentials.Token
import org.http4s.Header
import org.http4s.headers.Authorization

package object security {

  private[security] implicit class AccessTokenOps(accessToken: AccessToken) {

    lazy val toHeader: Header = accessToken match {
      case PersonalAccessToken(token) => Header("PRIVATE-TOKEN", token)
      case OAuthAccessToken(token)    => Authorization(Token(Bearer, token))
    }
  }
}

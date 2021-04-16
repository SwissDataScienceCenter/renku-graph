package ch.datascience.tokenrepository.repository

import ch.datascience.graph.model.projects
import cats.syntax.all._
import ch.datascience.tokenrepository.repository.AccessTokenCrypto.EncryptedAccessToken
import skunk.{Decoder, Encoder}
import skunk.codec.all.{int4, varchar}

private trait TokenRepositoryTypeSerializers {
  val projectIdGet: Decoder[projects.Id] = int4.map(projects.Id.apply)
  val projectIdPut: Encoder[projects.Id] = int4.values.contramap(_.value)

  val projectPathGet: Decoder[projects.Path] = varchar.map(projects.Path.apply)
  val projectPathPut: Encoder[projects.Path] =
    varchar.values.contramap((b: projects.Path) => b.value)

  val encryptedAccessTokenGet: Decoder[EncryptedAccessToken] =
    varchar.emap(s => EncryptedAccessToken.from(s).leftMap(_.getMessage))
  val encryptedAccessTokenPut: Encoder[EncryptedAccessToken] =
    varchar.values.contramap((b: EncryptedAccessToken) => b.value)
}

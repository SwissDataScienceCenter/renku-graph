package io.renku.cache

import cats.MonadThrow
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader
import pureconfig.error.{CannotConvert, ConfigReaderFailures}
import pureconfig.generic.auto._
import pureconfig.{ConfigReader, ConfigSource}

object CacheConfigLoader {

  implicit val evictStrategyReader: ConfigReader[EvictStrategy] =
    ConfigReader.fromString {
      case "least-recently-used" => Right(EvictStrategy.LeastRecentlyUsed)
      case "oldest"              => Right(EvictStrategy.Oldest)
      case str                   => Left(CannotConvert(str, "EvictStrategy", "Unknown value"))
    }

  def read(config: Config): Either[ConfigReaderFailures, CacheConfig] =
    ConfigSource.fromConfig(config).load[CacheConfig]

  def unsafeRead(config: Config): CacheConfig =
    ConfigSource.fromConfig(config).loadOrThrow[CacheConfig]

  def load[F[_]: MonadThrow](at: String, config: Config = ConfigFactory.load()) =
    ConfigLoader.find[F, CacheConfig](at, config)
}

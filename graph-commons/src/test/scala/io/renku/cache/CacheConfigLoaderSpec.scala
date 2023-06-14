package io.renku.cache

import com.typesafe.config.ConfigFactory
import io.renku.cache.CacheConfig.ClearConfig.Periodic
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import scala.concurrent.duration._

class CacheConfigLoaderSpec extends AnyWordSpec with should.Matchers {

  "CacheConfigLoader" should {
    "load from given config" in {
      val cfg = ConfigFactory.parseString("""evict-strategy = oldest
                                            |ignore-empty-values = true
                                            |ttl = 5s
                                            |clear-config = {
                                            |  type = periodic
                                            |  maximum-size = 1000
                                            |  interval = 10min
                                            |}
                                            |""".stripMargin)

      val cc = CacheConfigLoader.unsafeRead(cfg)
      cc shouldBe CacheConfig(EvictStrategy.Oldest, true, 5.seconds, Periodic(1000, 10.minutes))
    }
  }
}

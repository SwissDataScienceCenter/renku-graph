package ch.datascience.graph.model

import io.circe.Decoder
import io.circe.Decoder.decodeList

final case class RenkuVersionPair(cliVersion: CliVersion, schemaVersion: SchemaVersion)
    extends Product
    with Serializable

object RenkuVersionPair {
  implicit lazy val versionPairDecoder: Decoder[List[RenkuVersionPair]] = { topCursor =>
    val renkuVersionPairs: Decoder[RenkuVersionPair] = { cursor =>
      for {
        cliVersion    <- cursor.downField("cliVersion").downField("value").as[CliVersion]
        schemaVersion <- cursor.downField("schemaVersion").downField("value").as[SchemaVersion]
      } yield RenkuVersionPair(cliVersion, schemaVersion)
    }
    topCursor.downField("results").downField("bindings").as(decodeList(renkuVersionPairs))
  }
}

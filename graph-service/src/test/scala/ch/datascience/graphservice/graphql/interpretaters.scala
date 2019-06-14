package ch.datascience.graphservice.graphql

import cats.effect.IO
import ch.datascience.graphservice.config.GitLabBaseUrl
import ch.datascience.graphservice.graphql.lineage.LineageRepository
import ch.datascience.graphservice.rdfstore.RDFConnectionResource

private class IOLineageRepository(
    rdfConnectionResource: RDFConnectionResource[IO],
    gitLabBaseUrl:         GitLabBaseUrl
) extends LineageRepository[IO](rdfConnectionResource, gitLabBaseUrl)

/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.knowledgegraph.datasets.rest

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.model.datasets.{Description, Identifier, Name, PublishedDate}
import ch.datascience.http.rest.paging.Paging.PagedResultsFinder
import ch.datascience.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import ch.datascience.knowledgegraph.datasets.CreatorsFinder
import ch.datascience.knowledgegraph.datasets.model.DatasetPublishing
import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.DatasetSearchResult
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Query.Phrase
import ch.datascience.knowledgegraph.datasets.rest.DatasetsSearchEndpoint.Sort
import ch.datascience.rdfstore.{IORdfStoreClient, RdfStoreConfig, SparqlQuery}
import ch.datascience.tinytypes.constraints.NonNegativeInt
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import eu.timepit.refined.auto._
import io.chrisdavenport.log4cats.Logger

import scala.concurrent.ExecutionContext
import scala.language.higherKinds

private trait DatasetsFinder[Interpretation[_]] {
  def findDatasets(maybePhrase: Option[Phrase],
                   sort:        Sort.By,
                   paging:      PagingRequest): Interpretation[PagingResponse[DatasetSearchResult]]
}

private object DatasetsFinder {
  final case class DatasetSearchResult(
      id:               Identifier,
      name:             Name,
      maybeDescription: Option[Description],
      published:        DatasetPublishing,
      projectsCount:    ProjectsCount
  )

  final class ProjectsCount private (val value: Int) extends AnyVal with IntTinyType
  implicit object ProjectsCount extends TinyTypeFactory[ProjectsCount](new ProjectsCount(_)) with NonNegativeInt
}

private class IODatasetsFinder(
    rdfStoreConfig:          RdfStoreConfig,
    creatorsFinder:          CreatorsFinder,
    logger:                  Logger[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger)
    with DatasetsFinder[IO]
    with Paging[IO, DatasetSearchResult] {

  import IODatasetsFinder._
  import cats.implicits._
  import creatorsFinder._

  override def findDatasets(maybePhrase:   Option[Phrase],
                            sort:          Sort.By,
                            pagingRequest: PagingRequest): IO[PagingResponse[DatasetSearchResult]] = {
    implicit val resultsFinder: PagedResultsFinder[IO, DatasetSearchResult] = pagedResultsFinder(
      sparqlQuery(maybePhrase, sort),
      maybeCountQuery = Some(countQuery(maybePhrase))
    )
    for {
      page                 <- findPage(pagingRequest)
      datasetsWithCreators <- (page.results map addCreators).parSequence
      updatedPage          <- page.updateResults[IO](datasetsWithCreators)
    } yield updatedPage
  }

  private def sparqlQuery(maybePhrase: Option[Phrase], sort: Sort.By): SparqlQuery = SparqlQuery(
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    maybePhrase match {
      case Some(phrase) if phrase.value.trim != "*" =>
        s"""|SELECT ?identifier ?name ?maybeDescription ?maybePublishedDate ?projectsCount
            |WHERE {
            |  {
            |    ?datasetId schema:sameAs ?sameAs ;
            |               schema:dateCreated ?earliestCreated ;
            |               schema:name ?name ;
            |               schema:identifier ?identifier .
            |    OPTIONAL { ?datasetId schema:description ?maybeDescription } .
            |    OPTIONAL { ?datasetId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?sameAs (COUNT(DISTINCT ?dsId) AS ?projectsCount) (MIN(?dateCreated) AS ?earliestCreated)
            |      WHERE {
            |        ?dsId schema:sameAs ?sameAs {
            |          SELECT ?dsId
            |          WHERE {
            |            {
            |              SELECT ?dsId
            |              WHERE {
            |                ?dsId text:query (schema:name '$phrase') ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            } UNION {
            |              SELECT ?dsId
            |              WHERE {
            |                ?dsId text:query (schema:description '$phrase') ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            } UNION {
            |              SELECT ?dsId
            |              WHERE {
            |                ?personId text:query (schema:name '$phrase') ;
            |                          rdf:type <http://schema.org/Person> .
            |                ?dsId schema:creator ?personId ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            }
            |          }
            |          GROUP BY ?dsId
            |        }
            |        ?dsId schema:dateCreated ?dateCreated .
            |        FILTER NOT EXISTS {
            |          ?dsId schema:sameAs ?dsWithoutSameAsIdString {
            |            ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |            FILTER NOT EXISTS { ?dsWithoutSameAsId schema:sameAs ?nonExistingSameAs } .
            |            BIND (str(?dsWithoutSameAsId) AS ?dsWithoutSameAsIdString)
            |          }
            |        }
            |      }
            |      GROUP BY ?sameAs
            |    }
            |  } UNION {
            |    ?dsId schema:name ?name ;
            |          schema:identifier ?identifier .
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?dsId ((COUNT(?dsIdString) \\u002B 1) AS ?projectsCount)
            |      WHERE {
            |        ?derivedDsId schema:sameAs ?dsIdString {
            |          {
            |            SELECT ?dsId
            |            WHERE {
            |              {
            |                SELECT ?dsId
            |                WHERE {
            |                  ?dsId text:query (schema:name '$phrase') ;
            |                        rdf:type <http://schema.org/Dataset> .
            |                }
            |              } UNION {
            |                SELECT ?dsId
            |                WHERE {
            |                  ?dsId text:query (schema:description '$phrase') ;
            |                        rdf:type <http://schema.org/Dataset> .
            |                }
            |              } UNION {
            |                SELECT ?dsId
            |                WHERE {
            |                  ?personId text:query (schema:name '$phrase') ;
            |                            rdf:type <http://schema.org/Person> .
            |                  ?dsId schema:creator ?personId ;
            |                        rdf:type <http://schema.org/Dataset> .
            |                }
            |              }
            |            }
            |            GROUP BY ?dsId
            |          }
            |          FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |          BIND (str(?dsId) AS ?dsIdString)
            |        }
            |      }
            |      GROUP BY ?dsId
            |    }
            |  } UNION {
            |    ?dsId schema:name ?name ;
            |          schema:identifier ?identifier .
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?dsId (1 AS ?projectsCount)
            |      WHERE {
            |        {
            |          SELECT ?dsId
            |          WHERE {
            |            {
            |              SELECT ?dsId
            |              WHERE {
            |                ?dsId text:query (schema:name '$phrase') ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            } UNION {
            |              SELECT ?dsId
            |              WHERE {
            |                ?dsId text:query (schema:description '$phrase') ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            } UNION {
            |              SELECT ?dsId
            |              WHERE {
            |                ?personId text:query (schema:name '$phrase') ;
            |                          rdf:type <http://schema.org/Person> .
            |                ?dsId schema:creator ?personId ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            }
            |          }
            |          GROUP BY ?dsId
            |        }
            |        FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |        BIND (str(?dsId) AS ?dsIdString)
            |        FILTER NOT EXISTS { ?derivedDsId schema:sameAs ?dsIdString } .
            |      }
            |      GROUP BY ?dsId
            |    }
            |  }
            |}
            |${`ORDER BY`(sort)}
            |""".stripMargin
      case _ =>
        s"""|SELECT ?identifier ?name ?maybeDescription ?maybePublishedDate ?projectsCount
            |WHERE {
            |  {
            |    ?datasetId schema:sameAs ?sameAs ;
            |               schema:dateCreated ?earliestCreated ;
            |               schema:name ?name ;
            |               schema:identifier ?identifier .
            |    OPTIONAL { ?datasetId schema:description ?maybeDescription } .
            |    OPTIONAL { ?datasetId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?sameAs (COUNT(DISTINCT ?dsId) AS ?projectsCount) (MIN(?dateCreated) AS ?earliestCreated)
            |      WHERE {
            |        ?dsId rdf:type <http://schema.org/Dataset> ;
            |              schema:dateCreated ?dateCreated ;
            |              schema:sameAs ?sameAs .
            |        FILTER NOT EXISTS { 
            |          ?dsId schema:sameAs ?dsWithoutSameAsIdString {
            |            ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |            FILTER NOT EXISTS { ?dsWithoutSameAsId schema:sameAs ?nonExistingSameAs } .
            |            BIND (str(?dsWithoutSameAsId) AS ?dsWithoutSameAsIdString)
            |          } 
            |        }
            |      }
            |      GROUP BY ?sameAs
            |    }
            |  } UNION {
            |    ?dsId schema:name ?name ;
            |          schema:identifier ?identifier .
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?dsId ((COUNT(?dsIdString) \\u002B 1) AS ?projectsCount)
            |      WHERE {
            |        ?derivedDsId schema:sameAs ?dsIdString {
            |          ?dsId rdf:type <http://schema.org/Dataset> .
            |          FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |          BIND (str(?dsId) AS ?dsIdString)
            |        }
            |      }
            |      GROUP BY ?dsId
            |    }
            |  } UNION {
            |    ?dsId schema:name ?name ;
            |          schema:identifier ?identifier .
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?dsId (1 AS ?projectsCount)
            |      WHERE {
            |        ?dsId rdf:type <http://schema.org/Dataset> .
            |        FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |        BIND (str(?dsId) AS ?dsIdString)
            |        FILTER NOT EXISTS { ?derivedDsId schema:sameAs ?dsIdString } .
            |      }
            |      GROUP BY ?dsId
            |    }
            |  }
            |}
            |${`ORDER BY`(sort)}
            |""".stripMargin
    }
  )

  private def countQuery(maybePhrase: Option[Phrase]): SparqlQuery = SparqlQuery(
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    maybePhrase match {
      case Some(phrase) if phrase.value.trim != "*" =>
        s"""|SELECT ?smthToCount
            |WHERE {
            |  {
            |    SELECT (?sameAs AS ?smthToCount)
            |    WHERE {
            |      ?dsId schema:sameAs ?sameAs {
            |        SELECT ?dsId
            |        WHERE {
            |          {
            |            SELECT ?dsId
            |            WHERE {
            |              ?dsId text:query (schema:name '$phrase') ;
            |                    rdf:type <http://schema.org/Dataset> .
            |            }
            |          } UNION {
            |            SELECT ?dsId
            |            WHERE {
            |              ?dsId text:query (schema:description '$phrase') ;
            |                    rdf:type <http://schema.org/Dataset> .
            |            }
            |          } UNION {
            |            SELECT ?dsId
            |            WHERE {
            |              ?personId text:query (schema:name '$phrase') ;
            |                        rdf:type <http://schema.org/Person> .
            |              ?dsId schema:creator ?personId ;
            |                    rdf:type <http://schema.org/Dataset> .
            |            }
            |          }
            |        }
            |        GROUP BY ?dsId
            |      }
            |      FILTER NOT EXISTS {
            |        ?dsId schema:sameAs ?dsWithoutSameAsIdString {
            |          ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |          FILTER NOT EXISTS { ?dsWithoutSameAsId schema:sameAs ?nonExistingSameAs } .
            |          BIND (str(?dsWithoutSameAsId) AS ?dsWithoutSameAsIdString)
            |        }
            |      }
            |    }
            |    GROUP BY ?sameAs
            |  } UNION {
            |    SELECT (?dsId AS ?smthToCount)
            |    WHERE {
            |      ?derivedDsId schema:sameAs ?dsIdString {
            |        {
            |          SELECT ?dsId
            |          WHERE {
            |            {
            |              SELECT ?dsId
            |              WHERE {
            |                ?dsId text:query (schema:name '$phrase') ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            } UNION {
            |              SELECT ?dsId
            |              WHERE {
            |                ?dsId text:query (schema:description '$phrase') ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            } UNION {
            |              SELECT ?dsId
            |              WHERE {
            |                ?personId text:query (schema:name '$phrase') ;
            |                          rdf:type <http://schema.org/Person> .
            |                ?dsId schema:creator ?personId ;
            |                      rdf:type <http://schema.org/Dataset> .
            |              }
            |            }
            |          }
            |          GROUP BY ?dsId
            |        }
            |        FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |        BIND (str(?dsId) AS ?dsIdString)
            |      }
            |    }
            |    GROUP BY ?dsId
            |  } UNION {
            |    SELECT (?dsId AS ?smthToCount)
            |    WHERE {
            |      {
            |        SELECT ?dsId
            |        WHERE {
            |          {
            |            SELECT ?dsId
            |            WHERE {
            |              ?dsId text:query (schema:name '$phrase') ;
            |                    rdf:type <http://schema.org/Dataset> .
            |            }
            |          } UNION {
            |            SELECT ?dsId
            |            WHERE {
            |              ?dsId text:query (schema:description '$phrase') ;
            |                    rdf:type <http://schema.org/Dataset> .
            |            }
            |          } UNION {
            |            SELECT ?dsId
            |            WHERE {
            |              ?personId text:query (schema:name '$phrase') ;
            |                        rdf:type <http://schema.org/Person> .
            |              ?dsId schema:creator ?personId ;
            |                    rdf:type <http://schema.org/Dataset> .
            |            }
            |          }
            |        }
            |        GROUP BY ?dsId
            |      }
            |      FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |      BIND (str(?dsId) AS ?dsIdString)
            |      FILTER NOT EXISTS { ?derivedDsId schema:sameAs ?dsIdString } .
            |    }
            |    GROUP BY ?dsId
            |  }
            |}
            |""".stripMargin
      case _ =>
        s"""|SELECT ?smthToCount
            |WHERE {
            |  {
            |    SELECT (?sameAs AS ?smthToCount)
            |    WHERE {
            |      ?dsId rdf:type <http://schema.org/Dataset> ;
            |            schema:sameAs ?sameAs .
            |      FILTER NOT EXISTS {
            |        ?dsId schema:sameAs ?dsWithoutSameAsIdString {
            |          ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |          FILTER NOT EXISTS { ?dsWithoutSameAsId schema:sameAs ?nonExistingSameAs } .
            |          BIND (str(?dsWithoutSameAsId) AS ?dsWithoutSameAsIdString)
            |        }
            |      }
            |    }
            |    GROUP BY ?sameAs
            |  } UNION {
            |    SELECT (?dsId AS ?smthToCount)
            |    WHERE {
            |      ?derivedDsId schema:sameAs ?dsIdString {
            |        ?dsId rdf:type <http://schema.org/Dataset> .
            |        FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |        BIND (str(?dsId) AS ?dsIdString)
            |      }
            |    }
            |    GROUP BY ?dsId
            |  } UNION {
            |    SELECT (?dsId AS ?smthToCount)
            |    WHERE {
            |      ?dsId rdf:type <http://schema.org/Dataset> .
            |      FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |      BIND (str(?dsId) AS ?dsIdString)
            |      FILTER NOT EXISTS { ?derivedDsId schema:sameAs ?dsIdString } .
            |    }
            |    GROUP BY ?dsId
            |  }
            |}
            |""".stripMargin
    }
  )

  private def `ORDER BY`(sort: Sort.By): String = sort.property match {
    case Sort.NameProperty          => s"ORDER BY ${sort.direction}(?name)"
    case Sort.DatePublishedProperty => s"ORDER BY ${sort.direction}(?maybePublishedDate)"
    case Sort.ProjectsCountProperty => s"ORDER BY ${sort.direction}(?projectsCount)"
  }

  private lazy val addCreators: DatasetSearchResult => IO[DatasetSearchResult] =
    dataset =>
      findCreators(dataset.id)
        .map(creators => dataset.copy(published = dataset.published.copy(creators = creators)))
}

private object IODatasetsFinder {
  import ch.datascience.knowledgegraph.datasets.rest.DatasetsFinder.ProjectsCount
  import io.circe.Decoder

  implicit val recordDecoder: Decoder[DatasetSearchResult] = { cursor =>
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    for {
      id                 <- cursor.downField("identifier").downField("value").as[Identifier]
      name               <- cursor.downField("name").downField("value").as[Name]
      maybePublishedDate <- cursor.downField("maybePublishedDate").downField("value").as[Option[PublishedDate]]
      projectsCount      <- cursor.downField("projectsCount").downField("value").as[ProjectsCount]
      maybeDescription <- cursor
                           .downField("maybeDescription")
                           .downField("value")
                           .as[Option[String]]
                           .map(blankToNone)
                           .flatMap(toOption[Description])
    } yield DatasetSearchResult(
      id,
      name,
      maybeDescription,
      DatasetPublishing(maybePublishedDate, Set.empty),
      projectsCount
    )
  }
}

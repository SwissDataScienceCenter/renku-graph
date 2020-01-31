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
            |  { # finding datasets having the same sameAs but not pointing to a dataset id from a renku project
            |    SELECT ?identifier ?name ?maybeDescription ?maybePublishedDate (?smallProjectCounts AS ?projectsCount)
            |    WHERE {
            |      { # locating dataset created as first with certain sameAs
            |        SELECT ?sameAs ?smallProjectCounts (MIN(?dateCreated) AS ?earliestCreated)
            |        WHERE {
            |          ?dsId rdf:type <http://schema.org/Dataset> ;
            |                schema:sameAs/schema:url ?sameAs ;
            |                schema:dateCreated ?dateCreated { # grouping all sharing sameAs
            |                  SELECT ?sameAs (SUM(?projectsCnt) AS ?smallProjectCounts)
            |                  WHERE { # grouping datasets by id and sameAs - to make forks as one row
            |                    SELECT ?dsId ?sameAs (COUNT(DISTINCT ?prId) AS ?projectsCnt)
            |                    WHERE {
            |                      ?dsId schema:sameAs/schema:url ?sameAs {
            |                        SELECT ?dsId
            |                        WHERE {
            |                          {
            |                            SELECT ?dsId
            |                            WHERE {
            |                              ?dsId text:query (schema:name '$phrase') ;
            |                                    rdf:type <http://schema.org/Dataset> .
            |                            }
            |                          } UNION {
            |                            SELECT ?dsId
            |                            WHERE {
            |                              ?dsId text:query (schema:description '$phrase') ;
            |                                    rdf:type <http://schema.org/Dataset> .
            |                            }
            |                          } UNION {
            |                            SELECT ?dsId
            |                            WHERE {
            |                              ?personId text:query (schema:name '$phrase') ;
            |                                        rdf:type <http://schema.org/Person> .
            |                              ?dsId schema:creator ?personId ;
            |                                    rdf:type <http://schema.org/Dataset> .
            |                            }
            |                          }
            |                        }
            |                        GROUP BY ?dsId
            |                      }
            |                      ?dsId schema:isPartOf ?prId
            |                      FILTER NOT EXISTS {
            |                        ?dsId schema:sameAs/schema:url ?dsWithoutSameAsId {
            |                          ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |                        }
            |                      }
            |                    }
            |                    GROUP BY ?dsId ?sameAs
            |                    HAVING (COUNT(*) > 0)
            |                  }
            |                  GROUP BY ?sameAs
            |                  HAVING (COUNT(*) > 0)
            |                }
            |        }
            |        GROUP BY ?sameAs ?smallProjectCounts
            |        HAVING (COUNT(*) > 0)
            |      } {
            |        ?datasetId schema:sameAs/schema:url ?sameAs ;
            |                   schema:dateCreated ?earliestCreated ;
            |                   schema:name ?name ;
            |                   schema:identifier ?identifier .
            |        OPTIONAL { ?datasetId schema:description ?maybeDescription } .
            |        OPTIONAL { ?datasetId schema:datePublished ?maybePublishedDate }
            |      }
            |    }
            |    GROUP BY ?identifier ?name ?maybeDescription ?maybePublishedDate ?smallProjectCounts
            |    HAVING (COUNT(*) > 0)
            |  } UNION { # finding datasets having the sameAs pointing to a dataset from a renku project
            |    SELECT ?name ?identifier ?maybeDescription ?maybePublishedDate (COUNT(DISTINCT ?projectId) AS ?projectsCount)
            |    WHERE {
            |      {
            |        SELECT ?dsId ?name ?identifier ?maybeDescription ?maybePublishedDate
            |        WHERE {
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
            |            HAVING (COUNT(*) > 0)
            |          } {
            |            ?derivedDsId schema:sameAs/schema:url ?dsId.
            |            ?dsId rdf:type <http://schema.org/Dataset>;
            |                  schema:name ?name ;
            |                  schema:identifier ?identifier .
            |            OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |            OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
            |          }
            |        }
            |        GROUP BY ?dsId ?name ?identifier ?maybeDescription ?maybePublishedDate
            |        HAVING (COUNT(*) > 0)
            |      } {
            |        ?datasetId schema:sameAs/schema:url ?dsId;
            |                   schema:isPartOf ?projectId
            |      } UNION {
            |        ?dsId schema:isPartOf ?projectId
            |        BIND (?dsId AS ?datasetId)
            |      }
            |    }
            |    GROUP BY ?name ?identifier ?maybeDescription ?maybePublishedDate
            |    HAVING (COUNT(*) > 0)
            |  } UNION { # finding datasets having no sameAs set and not imported to another projects
            |    ?dsId schema:name ?name ;
            |          schema:identifier ?identifier .
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?dsId (COUNT(DISTINCT ?projectId) AS ?projectsCount)
            |      WHERE {
            |        ?dsId rdf:type <http://schema.org/Dataset>;
            |              schema:isPartOf ?projectId {
            |                SELECT ?dsId
            |                WHERE {
            |                  {
            |                    SELECT ?dsId
            |                    WHERE {
            |                      ?dsId text:query (schema:name '$phrase') ;
            |                            rdf:type <http://schema.org/Dataset> .
            |                    }
            |                  } UNION {
            |                    SELECT ?dsId
            |                    WHERE {
            |                      ?dsId text:query (schema:description '$phrase') ;
            |                            rdf:type <http://schema.org/Dataset> .
            |                    }
            |                  } UNION {
            |                    SELECT ?dsId
            |                    WHERE {
            |                      ?personId text:query (schema:name '$phrase') ;
            |                                rdf:type <http://schema.org/Person> .
            |                      ?dsId schema:creator ?personId ;
            |                            rdf:type <http://schema.org/Dataset> .
            |                    }
            |                  }
            |                }
            |                GROUP BY ?dsId
            |              }
            |              FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |              FILTER NOT EXISTS { ?derivedDsId schema:sameAs/schema:url ?dsId } .
            |      }
            |      GROUP BY ?dsId
            |      HAVING (COUNT(*) > 0)
            |    }
            |  }
            |}
            |${`ORDER BY`(sort)}
            |""".stripMargin
      case _ =>
        s"""|SELECT ?identifier ?name ?maybeDescription ?maybePublishedDate ?projectsCount
            |WHERE {
            |  { # finding datasets having the same sameAs but not pointing to a dataset id from a renku project
            |    SELECT ?identifier ?name ?maybeDescription ?maybePublishedDate (?smallProjectCounts AS ?projectsCount)
            |    WHERE {
            |      { # locating dataset created as first with certain sameAs
            |        SELECT ?sameAs ?smallProjectCounts (MIN(?dateCreated) AS ?earliestCreated)
            |        WHERE {
            |          ?dsId rdf:type <http://schema.org/Dataset> ;
            |                schema:sameAs/schema:url ?sameAs ;
            |                schema:dateCreated ?dateCreated { # grouping all sharing sameAs
            |                  SELECT ?sameAs (SUM(?projectsCnt) AS ?smallProjectCounts)
            |                  WHERE { # grouping datasets by id and sameAs - to make forks as one row
            |                    SELECT ?dsId ?sameAs (COUNT(DISTINCT ?prId) AS ?projectsCnt)
            |                    WHERE {
            |                      ?dsId rdf:type <http://schema.org/Dataset> ;
            |                            schema:isPartOf ?prId ;
            |                            schema:sameAs/schema:url ?sameAs .
            |                      FILTER NOT EXISTS {
            |                        ?dsId schema:sameAs/schema:url ?dsWithoutSameAsId {
            |                          ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |                        }
            |                      }
            |                    }
            |                    GROUP BY ?dsId ?sameAs
            |                    HAVING (COUNT(*) > 0)
            |                  }
            |                  GROUP BY ?sameAs
            |                  HAVING (COUNT(*) > 0)
            |                }
            |        }
            |        GROUP BY ?sameAs ?smallProjectCounts
            |        HAVING (COUNT(*) > 0)
            |      } {
            |        ?datasetId schema:sameAs/schema:url ?sameAs ;
            |                   schema:dateCreated ?earliestCreated ;
            |                   schema:name ?name ;
            |                   schema:identifier ?identifier .
            |        OPTIONAL { ?datasetId schema:description ?maybeDescription } .
            |        OPTIONAL { ?datasetId schema:datePublished ?maybePublishedDate }
            |      }
            |    }
            |    GROUP BY ?identifier ?name ?maybeDescription ?maybePublishedDate ?smallProjectCounts
            |    HAVING (COUNT(*) > 0)
            |  } UNION { # finding datasets having the sameAs pointing to a dataset from a renku project
            |    SELECT ?name ?identifier ?maybeDescription ?maybePublishedDate (COUNT(DISTINCT ?projectId) AS ?projectsCount)
            |    WHERE {
            |      {
            |        SELECT ?sourceDsId ?name ?identifier ?maybeDescription ?maybePublishedDate
            |        WHERE {
            |          ?derivedDsId schema:sameAs/schema:url ?sourceDsId {
            |            ?sourceDsId rdf:type <http://schema.org/Dataset>;
            |                        schema:name ?name ;
            |                        schema:identifier ?identifier .
            |            OPTIONAL { ?sourceDsId schema:description ?maybeDescription } .
            |            OPTIONAL { ?sourceDsId schema:datePublished ?maybePublishedDate }
            |          }
            |        }
            |        GROUP BY ?sourceDsId ?name ?identifier ?maybeDescription ?maybePublishedDate
            |      }
            |      {
            |        ?dsId schema:sameAs/schema:url ?sourceDsId;
            |              schema:isPartOf ?projectId
            |      } UNION {
            |        ?sourceDsId schema:isPartOf ?projectId
            |        BIND (?sourceDsId AS ?dsId)
            |      }
            |    }
            |    GROUP BY ?name ?identifier ?maybeDescription ?maybePublishedDate
            |    HAVING (COUNT(*) > 0)
            |  } UNION { # finding datasets having no sameAs set and not imported to another projects
            |    ?dsId schema:name ?name ;
            |          schema:identifier ?identifier .
            |    OPTIONAL { ?dsId schema:description ?maybeDescription } .
            |    OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate } {
            |      SELECT ?dsId (COUNT(DISTINCT ?projectId) AS ?projectsCount)
            |      WHERE {
            |        ?dsId rdf:type <http://schema.org/Dataset>;
            |              schema:isPartOf ?projectId.
            |        FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |        FILTER NOT EXISTS { ?derivedDsId schema:sameAs/schema:url ?dsId } .
            |      }
            |      GROUP BY ?dsId
            |      HAVING (COUNT(*) > 0)
            |    }
            |  }
            |}
            |GROUP BY ?identifier ?name ?maybeDescription ?maybePublishedDate ?projectsCount
            |HAVING (COUNT(*) > 0)
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
            |      ?dsId schema:sameAs/schema:url ?sameAs {
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
            |        ?dsId schema:sameAs/schema:url ?dsWithoutSameAsId {
            |          ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |        }
            |      }
            |    }
            |    GROUP BY ?sameAs
            |  } UNION {
            |    SELECT (?dsId AS ?smthToCount)
            |    WHERE {
            |      ?derivedDsId schema:sameAs/schema:url ?dsId {
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
            |      FILTER NOT EXISTS { ?derivedDsId schema:sameAs/schema:url ?dsId } .
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
            |            schema:sameAs/schema:url ?sameAs .
            |      FILTER NOT EXISTS {
            |        ?dsId schema:sameAs/schema:url ?dsWithoutSameAsId {
            |          ?dsWithoutSameAsId rdf:type <http://schema.org/Dataset> .
            |          FILTER NOT EXISTS { ?dsWithoutSameAsId schema:sameAs ?nonExistingSameAs } .
            |        }
            |      }
            |    }
            |    GROUP BY ?sameAs
            |  } UNION {
            |    SELECT (?dsId AS ?smthToCount)
            |    WHERE {
            |      ?derivedDsId schema:sameAs/schema:url ?dsId {
            |        ?dsId rdf:type <http://schema.org/Dataset> .
            |        FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |      }
            |    }
            |    GROUP BY ?dsId
            |  } UNION {
            |    SELECT (?dsId AS ?smthToCount)
            |    WHERE {
            |      ?dsId rdf:type <http://schema.org/Dataset> .
            |      FILTER NOT EXISTS { ?dsId schema:sameAs ?nonExistingSameAs } .
            |      FILTER NOT EXISTS { ?derivedDsId schema:sameAs/schema:url ?dsId } .
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

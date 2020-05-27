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
import ch.datascience.rdfstore._
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
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with DatasetsFinder[IO]
    with Paging[IO, DatasetSearchResult] {

  import IODatasetsFinder._
  import cats.implicits._
  import creatorsFinder._

  override def findDatasets(maybePhrase:   Option[Phrase],
                            sort:          Sort.By,
                            pagingRequest: PagingRequest): IO[PagingResponse[DatasetSearchResult]] = {
    val phrase = maybePhrase getOrElse Phrase("*")
    implicit val resultsFinder: PagedResultsFinder[IO, DatasetSearchResult] = pagedResultsFinder(
      sparqlQuery(phrase, sort),
      maybeCountQuery = Some(countQuery(phrase))
    )
    for {
      page                 <- findPage(pagingRequest)
      datasetsWithCreators <- (page.results map addCreators).parSequence
      updatedPage          <- page.updateResults[IO](datasetsWithCreators)
    } yield updatedPage
  }

  private def sparqlQuery(phrase: Phrase, sort: Sort.By): SparqlQuery = SparqlQuery(
    name = "ds free-text search",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    s"""|SELECT DISTINCT ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs ?projectsCount
        |WHERE {
        |  {
        |    SELECT ?topmostDsId ?groupingIdentifier (COUNT(DISTINCT ?projectId) AS ?projectsCount) (?case AS ?recordCase)
        |    WHERE {
        |      { # finding all ds having neither sameAs nor wasDerivedFrom
        |        ?dsId rdf:type <http://schema.org/Dataset>;
        |              schema:isPartOf ?projectId.
        |        FILTER NOT EXISTS { ?dsId prov:wasDerivedFrom ?otherDsId1 }
        |        FILTER NOT EXISTS { ?otherDsId2 prov:wasDerivedFrom ?dsId }
        |        FILTER NOT EXISTS { ?dsId schema:sameAs/schema:url ?otherDsId3 }
        |        FILTER NOT EXISTS { ?otherDsId4 schema:sameAs/schema:url ?dsId }
        |        BIND (?dsId AS ?topmostDsId)
        |        BIND (?dsId AS ?groupingIdentifier)
        |        BIND (3 AS ?case)
        |      } UNION { # finding all ds with the import hierarchy
        |        SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (2 AS ?case)
        |        WHERE {
        |          {
        |            SELECT ?luceneDsId
        |            WHERE {
        |              {
        |                SELECT ?id
        |                WHERE { ?id text:query (schema:name schema:description '$phrase') }
        |                GROUP BY ?id
        |                HAVING (COUNT(*) > 0)
        |              } {
        |                ?id rdf:type <http://schema.org/Dataset>
        |                BIND (?id AS ?luceneDsId)
        |              } UNION {
        |                ?id rdf:type <http://schema.org/Person>.
        |                ?luceneDsId schema:creator ?id;
        |                            rdf:type <http://schema.org/Dataset>.
        |              }
        |            }
        |            GROUP BY ?luceneDsId
        |            HAVING (COUNT(*) > 0)
        |          }
        |          filter (?luceneDsId = ?foundDsId) {
        |            {
        |              SELECT ?topmostSameAs (MIN(?dateCreated) as ?minDateCreated)
        |              WHERE {
        |                {
        |                  SELECT (?childId AS ?topmostSameAs)
        |                  WHERE {
        |                    {
        |                      SELECT DISTINCT ?sameAs
        |                      WHERE {
        |                      ?someDsId rdf:type <http://schema.org/Dataset>;
        |                                schema:sameAs/schema:url ?sameAs.
        |                      }
        |                    } {
        |                      ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
        |                      ?childId (schema:sameAs/schema:url)* ?parentDsId.
        |                    }
        |                  }
        |                  GROUP BY ?childId
        |                  HAVING (count(?parentDsId) = 1)
        |                } {
        |                  ?dsId schema:sameAs/schema:url ?topmostSameAs;
        |                        prov:qualifiedGeneration/prov:activity ?activityId .
        |                  ?activityId prov:startedAtTime ?dateCreated
        |                } UNION {
        |                  ?topmostSameAs rdf:type <http://schema.org/Dataset>;
        |                                 schema:identifier ?id .
        |                  ?dsId schema:identifier ?id;
        |                        prov:qualifiedGeneration/prov:activity ?activityId .
        |                  ?activityId prov:startedAtTime ?dateCreated
        |                }
        |              }
        |              GROUP BY ?topmostSameAs
        |              HAVING (COUNT(*) > 0)
        |            } {
        |              ?dsId schema:sameAs/schema:url ?topmostSameAs;
        |                    prov:qualifiedGeneration/prov:activity ?activityId.
        |              ?activityId prov:startedAtTime ?minDateCreated
        |              BIND (?topmostSameAs AS ?sameAs)
        |              BIND (?dsId AS ?topmostDsId)
        |            } UNION {
        |              ?topmostSameAs rdf:type <http://schema.org/Dataset>;
        |                             schema:identifier ?identifier .
        |              ?dsId schema:identifier ?identifier;
        |                    prov:qualifiedGeneration/prov:activity ?activityId.
        |              ?activityId prov:startedAtTime ?minDateCreated
        |              BIND (?topmostSameAs AS ?sameAs)
        |              BIND (?topmostSameAs AS ?topmostDsId)
        |            }
        |          } {
        |            ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
        |            ?childId (schema:sameAs/schema:url)* ?parentDsId.
        |          } {
        |            ?foundDsId rdf:type <http://schema.org/Dataset>;
        |                       schema:sameAs/schema:url ?sameAs;
        |                       schema:isPartOf ?projectId.
        |          } UNION {
        |            ?sameAs rdf:type <http://schema.org/Dataset>;
        |                    schema:isPartOf ?projectId.
        |            BIND (?sameAs AS ?foundDsId)
        |          } 
        |          # getting rid of those ds which are either deriving or being derived
        |          FILTER NOT EXISTS { 
        |            ?foundDsId prov:wasDerivedFrom ?ds1Id.
        |            FILTER EXISTS { ?ds1Id schema:isPartOf ?projectId }
        |          }
        |          FILTER NOT EXISTS { 
        |            ?ds2Id prov:wasDerivedFrom ?foundDsId.
        |            FILTER EXISTS { ?ds2Id schema:isPartOf ?projectId }
        |          }
        |        }
        |      } UNION { # finding all ds with the modification hierarchy
        |        SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (1 AS ?case)
        |        WHERE {
        |          {
        |            SELECT ?luceneDsId
        |            WHERE {
        |              {
        |                SELECT ?id
        |                WHERE { ?id text:query (schema:name schema:description '$phrase') }
        |                GROUP BY ?id
        |                HAVING (COUNT(*) > 0)
        |              } {
        |                ?id rdf:type <http://schema.org/Dataset>
        |                BIND (?id AS ?luceneDsId)
        |              } UNION {
        |                ?id rdf:type <http://schema.org/Person>.
        |                ?luceneDsId schema:creator ?id;
        |                            rdf:type <http://schema.org/Dataset>.
        |              }
        |            }
        |            GROUP BY ?luceneDsId
        |            HAVING (COUNT(*) > 0)
        |          }
        |          filter (?luceneDsId = ?dsId) {
        |            {
        |              SELECT (?childId AS ?dsId) ?topmostDsId
        |              WHERE {
        |                {
        |                  SELECT ?topmostDsId (MAX(?depth) AS ?maxDepth)
        |                  WHERE {
        |                    SELECT ?topmostDsId ?childId (count(?parentDsId) - 1 as ?depth)
        |                    WHERE {
        |                      {
        |                        SELECT (?childId AS ?topmostDsId)
        |                        WHERE {
        |                          {
        |                            SELECT DISTINCT ?dsIdToCheck
        |                            WHERE {
        |                              ?someDsId rdf:type <http://schema.org/Dataset>;
        |                                        prov:wasDerivedFrom ?dsIdToCheck.
        |                            }
        |                          } {
        |                            ?parentDsId prov:wasDerivedFrom* ?dsIdToCheck.
        |                            ?childId prov:wasDerivedFrom* ?parentDsId.
        |                          }
        |                        }
        |                        GROUP BY ?childId
        |                        HAVING (count(?parentDsId) = 1)
        |                      } {
        |                        ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
        |                        ?childId prov:wasDerivedFrom* ?parentDsId.
        |                      }
        |                    }
        |                    GROUP BY ?topmostDsId ?childId
        |                  }
        |                  GROUP BY ?topmostDsId
        |                } {
        |                    ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
        |                    ?childId prov:wasDerivedFrom* ?parentDsId.
        |                }
        |              }
        |              GROUP BY ?topmostDsId ?childId ?maxDepth
        |              HAVING (count(?parentDsId) - 1 = ?maxDepth)
        |            } {
        |              ?dsId rdf:type <http://schema.org/Dataset>;
        |                    schema:isPartOf ?projectId.
        |            }
        |          }
        |        }
        |      }
        |    }
        |    GROUP BY ?topmostDsId ?groupingIdentifier ?case
        |  }
        |  FILTER (?topmostDsIdInner = ?topmostDsId && ?caseInner = ?recordCase) {
        |    SELECT (?topmostDsId AS ?topmostDsIdInner) ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs (?case AS ?caseInner)
        |    WHERE {
        |      { # finding all ds having neither sameAs nor wasDerivedFrom
        |            ?dsId rdf:type <http://schema.org/Dataset>.
        |            FILTER NOT EXISTS { ?dsId prov:wasDerivedFrom ?otherDsId1 }
        |            FILTER NOT EXISTS { ?otherDsId2 prov:wasDerivedFrom ?dsId }
        |            FILTER NOT EXISTS { ?dsId schema:sameAs/schema:url ?otherDsId3 }
        |            FILTER NOT EXISTS { ?otherDsId4 schema:sameAs/schema:url ?dsId }
        |            ?dsId rdf:type <http://schema.org/Dataset>;
        |                  schema:identifier ?identifier;
        |                  schema:name ?name.
        |            OPTIONAL { ?dsId schema:description ?maybeDescription }
        |            OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
        |            OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
        |            OPTIONAL { ?dsId schema:url ?maybeUrl }
        |            BIND (IF (BOUND(?maybeUrl), ?maybeUrl, ?dsId) as ?sameAs)
        |            BIND (?dsId AS ?topmostDsId)
        |            BIND (3 AS ?case)
        |      } UNION { # finding all ds with the import hierarchy
        |        SELECT DISTINCT ?topmostDsId ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs (2 AS ?case)
        |        WHERE {
        |          {
        |            {
        |              SELECT ?topmostSameAs (MIN(?dateCreated) as ?minDateCreated)
        |              WHERE {
        |                {
        |                  SELECT (?childId AS ?topmostSameAs)
        |                  WHERE {
        |                    {
        |                      SELECT DISTINCT ?sameAs
        |                      WHERE {
        |                      ?someDsId rdf:type <http://schema.org/Dataset>;
        |                                schema:sameAs/schema:url ?sameAs.
        |                      }
        |                    } {
        |                      ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
        |                      ?childId (schema:sameAs/schema:url)* ?parentDsId.
        |                    }
        |                  }
        |                  GROUP BY ?childId
        |                  HAVING (count(?parentDsId) = 1)
        |                } {
        |                  ?dsId schema:sameAs/schema:url ?topmostSameAs;
        |                        prov:qualifiedGeneration/prov:activity ?activityId .
        |                  ?activityId prov:startedAtTime ?dateCreated
        |                } UNION {
        |                  ?topmostSameAs rdf:type <http://schema.org/Dataset>;
        |                                 schema:identifier ?id .
        |                  ?dsId schema:identifier ?id;
        |                        prov:qualifiedGeneration/prov:activity ?activityId .
        |                  ?activityId prov:startedAtTime ?dateCreated
        |                }
        |              }
        |              GROUP BY ?topmostSameAs
        |            } {
        |              ?dsId schema:sameAs/schema:url ?topmostSameAs;
        |                    schema:identifier ?identifier;
        |                    schema:name ?name;
        |                    prov:qualifiedGeneration/prov:activity ?activityId.
        |              ?activityId prov:startedAtTime ?minDateCreated
        |              OPTIONAL { ?dsId schema:description ?maybeDescription }
        |              OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
        |              OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
        |              OPTIONAL { ?dsId schema:url ?maybeUrl }
        |              BIND (?topmostSameAs AS ?sameAs)
        |              BIND (?dsId AS ?topmostDsId)
        |            } UNION {
        |              ?topmostSameAs rdf:type <http://schema.org/Dataset>;
        |                             schema:identifier ?identifier .
        |              ?dsId schema:identifier ?identifier;
        |                    schema:name ?name;
        |                    prov:qualifiedGeneration/prov:activity ?activityId.
        |              ?activityId prov:startedAtTime ?minDateCreated
        |              OPTIONAL { ?dsId schema:description ?maybeDescription }
        |              OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
        |              OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
        |              OPTIONAL { ?dsId schema:url ?maybeUrl }
        |              BIND (?topmostSameAs AS ?sameAs)
        |              BIND (?topmostSameAs AS ?topmostDsId)
        |            }
        |          }
        |        }
        |      }  UNION { # finding all ds with the modification hierarchy
        |        SELECT ?topmostDsId ?identifier ?name ?maybeDescription ?maybePublishedDate ?maybeDerivedFrom ?sameAs (1 AS ?case)
        |        WHERE {
        |          {
        |            SELECT (?childId AS ?dsId) ?topmostDsId
        |            WHERE {
        |              {
        |                SELECT ?topmostDsId (MAX(?depth) AS ?maxDepth)
        |                WHERE {
        |                  SELECT ?topmostDsId ?childId (count(?parentDsId) - 1 as ?depth)
        |                  WHERE {
        |                    {
        |                      SELECT (?childId AS ?topmostDsId)
        |                      WHERE {
        |                        {
        |                          SELECT DISTINCT ?dsIdToCheck
        |                          WHERE {
        |                            ?someDsId rdf:type <http://schema.org/Dataset>;
        |                                      prov:wasDerivedFrom ?dsIdToCheck.
        |                          }
        |                        } {
        |                          ?parentDsId prov:wasDerivedFrom* ?dsIdToCheck.
        |                          ?childId prov:wasDerivedFrom* ?parentDsId.
        |                        }
        |                      }
        |                      GROUP BY ?childId
        |                      HAVING (count(?parentDsId) = 1)
        |                    } {
        |                      ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
        |                      ?childId prov:wasDerivedFrom* ?parentDsId.
        |                    }
        |                  }
        |                  GROUP BY ?topmostDsId ?childId
        |                }
        |                GROUP BY ?topmostDsId
        |              } {
        |                  ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
        |                  ?childId prov:wasDerivedFrom* ?parentDsId.
        |              }
        |            }
        |            GROUP BY ?topmostDsId ?childId ?maxDepth
        |            HAVING (count(?parentDsId) - 1 = ?maxDepth)
        |          } {
        |            ?dsId rdf:type <http://schema.org/Dataset>;
        |                  schema:identifier ?identifier;
        |                  schema:name ?name.
        |            OPTIONAL { ?dsId schema:description ?maybeDescription }
        |            OPTIONAL { ?dsId schema:datePublished ?maybePublishedDate }
        |            OPTIONAL { ?dsId prov:wasDerivedFrom ?maybeDerivedFrom }
        |            OPTIONAL { ?dsId schema:url ?maybeUrl }
        |            BIND (IF (BOUND(?maybeUrl), ?maybeUrl, ?dsId) as ?sameAs)
        |          }
        |        }
        |      }
        |    }
        |  }
        |}
        |${`ORDER BY`(sort)}
        |""".stripMargin
  )

  private def countQuery(phrase: Phrase): SparqlQuery = SparqlQuery(
    name = "ds free-text search - count",
    Set(
      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
      "PREFIX schema: <http://schema.org/>",
      "PREFIX prov: <http://www.w3.org/ns/prov#>",
      "PREFIX text: <http://jena.apache.org/text#>"
    ),
    s"""|SELECT ?topmostDsId ?groupingIdentifier
        |WHERE {
        |  { # finding all ds having neither sameAs nor wasDerivedFrom
        |    ?dsId rdf:type <http://schema.org/Dataset>;
        |          schema:isPartOf ?projectId.
        |    FILTER NOT EXISTS { ?dsId prov:wasDerivedFrom ?otherDsId1 }
        |    FILTER NOT EXISTS { ?otherDsId2 prov:wasDerivedFrom ?dsId }
        |    FILTER NOT EXISTS { ?dsId schema:sameAs/schema:url ?otherDsId3 }
        |    FILTER NOT EXISTS { ?otherDsId4 schema:sameAs/schema:url ?dsId }
        |    BIND (?dsId AS ?topmostDsId)
        |    BIND (?dsId AS ?groupingIdentifier)
        |    BIND (3 AS ?case)
        |  } UNION { # finding all ds with the import hierarchy
        |    SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (2 AS ?case)
        |    WHERE {
        |      {
        |        SELECT ?luceneDsId
        |        WHERE {
        |          ?id text:query (schema:name schema:description '$phrase')
        |          {
        |            ?id rdf:type <http://schema.org/Dataset>
        |            BIND (?id AS ?luceneDsId)
        |          } UNION {
        |            ?id rdf:type <http://schema.org/Person>.
        |            ?luceneDsId schema:creator ?id;
        |                        rdf:type <http://schema.org/Dataset>.
        |          }
        |        }
        |        GROUP BY ?luceneDsId
        |        HAVING (COUNT(*) > 0)
        |      }
        |      filter (?luceneDsId = ?foundDsId) {
        |        {
        |          SELECT ?topmostSameAs (MIN(?dateCreated) as ?minDateCreated)
        |          WHERE {
        |            {
        |              SELECT (?childId AS ?topmostSameAs)
        |              WHERE {
        |                {
        |                  SELECT DISTINCT ?sameAs
        |                  WHERE {
        |                  ?someDsId rdf:type <http://schema.org/Dataset>;
        |                            schema:sameAs/schema:url ?sameAs.
        |                  }
        |                } {
        |                  ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
        |                  ?childId (schema:sameAs/schema:url)* ?parentDsId.
        |                }
        |              }
        |              GROUP BY ?childId
        |              HAVING (count(?parentDsId) = 1)
        |            } {
        |              ?dsId schema:sameAs/schema:url ?topmostSameAs;
        |                    prov:qualifiedGeneration/prov:activity ?activityId .
        |              ?activityId prov:startedAtTime ?dateCreated
        |            } UNION {
        |              ?topmostSameAs rdf:type <http://schema.org/Dataset>;
        |                             schema:identifier ?id .
        |              ?dsId schema:identifier ?id;
        |                    prov:qualifiedGeneration/prov:activity ?activityId .
        |              ?activityId prov:startedAtTime ?dateCreated
        |            }
        |          }
        |          GROUP BY ?topmostSameAs
        |          HAVING (COUNT(*) > 0)
        |        } {
        |          ?dsId schema:sameAs/schema:url ?topmostSameAs;
        |                prov:qualifiedGeneration/prov:activity ?activityId.
        |          ?activityId prov:startedAtTime ?minDateCreated
        |          BIND (?topmostSameAs AS ?sameAs)
        |          BIND (?dsId AS ?topmostDsId)
        |        } UNION {
        |          ?topmostSameAs rdf:type <http://schema.org/Dataset>;
        |                         schema:identifier ?identifier .
        |          ?dsId schema:identifier ?identifier;
        |                prov:qualifiedGeneration/prov:activity ?activityId.
        |          ?activityId prov:startedAtTime ?minDateCreated
        |          BIND (?topmostSameAs AS ?sameAs)
        |          BIND (?topmostSameAs AS ?topmostDsId)
        |        }
        |      } {
        |        ?parentDsId (schema:sameAs/schema:url)* ?sameAs.
        |        ?childId (schema:sameAs/schema:url)* ?parentDsId.
        |      } {
        |        ?foundDsId rdf:type <http://schema.org/Dataset>;
        |                   schema:sameAs/schema:url ?sameAs;
        |                   schema:isPartOf ?projectId.
        |      } UNION {
        |        ?sameAs rdf:type <http://schema.org/Dataset>;
        |                schema:isPartOf ?projectId.
        |        BIND (?sameAs AS ?foundDsId)
        |      }
        |      # getting rid of those ds which are either deriving or being derived
        |      FILTER NOT EXISTS { 
        |        ?foundDsId prov:wasDerivedFrom ?ds1Id.
        |        FILTER EXISTS { ?ds1Id schema:isPartOf ?projectId }
        |      }
        |      FILTER NOT EXISTS { 
        |        ?ds2Id prov:wasDerivedFrom ?foundDsId.
        |        FILTER EXISTS { ?ds2Id schema:isPartOf ?projectId }
        |      }
        |    }
        |  } UNION { # finding all ds with the modification hierarchy
        |    SELECT DISTINCT ?topmostDsId (?topmostDsId AS ?groupingIdentifier) ?projectId (1 AS ?case)
        |    WHERE {
        |      {
        |        SELECT (?childId AS ?dsId) ?topmostDsId
        |        WHERE {
        |          {
        |            SELECT ?topmostDsId (MAX(?depth) AS ?maxDepth)
        |            WHERE {
        |              SELECT ?topmostDsId ?childId (count(?parentDsId) - 1 as ?depth)
        |              WHERE {
        |                {
        |                  SELECT (?childId AS ?topmostDsId)
        |                  WHERE {
        |                    {
        |                      SELECT DISTINCT ?dsIdToCheck
        |                      WHERE {
        |                        ?someDsId rdf:type <http://schema.org/Dataset>;
        |                                  prov:wasDerivedFrom ?dsIdToCheck.
        |                      }
        |                    } {
        |                      ?parentDsId prov:wasDerivedFrom* ?dsIdToCheck.
        |                      ?childId prov:wasDerivedFrom* ?parentDsId.
        |                    }
        |                  }
        |                  GROUP BY ?childId
        |                  HAVING (count(?parentDsId) = 1)
        |                } {
        |                  ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
        |                  ?childId prov:wasDerivedFrom* ?parentDsId.
        |                }
        |              }
        |              GROUP BY ?topmostDsId ?childId
        |            }
        |            GROUP BY ?topmostDsId
        |          } {
        |              ?parentDsId prov:wasDerivedFrom* ?topmostDsId.
        |              ?childId prov:wasDerivedFrom* ?parentDsId.
        |          }
        |        }
        |        GROUP BY ?topmostDsId ?childId ?maxDepth
        |        HAVING (count(?parentDsId) - 1 = ?maxDepth)
        |      } {
        |        ?dsId rdf:type <http://schema.org/Dataset>;
        |              schema:isPartOf ?projectId.
        |      }
        |    }
        |  }
        |}
        |GROUP BY ?topmostDsId ?groupingIdentifier ?case
        |""".stripMargin
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

/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.docs

import cats.MonadThrow
import cats.implicits._
import io.circe.literal._
import io.renku.http.ErrorMessage
import io.renku.http.InfoMessage._
import io.renku.knowledgegraph.docs.model.Operation.GET
import io.renku.knowledgegraph.docs.model._

trait EndpointDocs {
  def path: Path
}

private object EndpointDocs {
  def apply[F[_]: MonadThrow]: F[EndpointDocs] = new EndpointDocsImpl().pure[F].widen
}

private class EndpointDocsImpl() extends EndpointDocs {

  override lazy val path: Path = Path(
    "OpenAPI specification of the service's resources",
    description = None,
    GET(
      Uri / "spec.json",
      Status.Ok -> Response("Specification in JSON",
                            Contents(MediaType.`application/json`("Sample response", example))
      ),
      Status.InternalServerError -> Response("Error",
                                             Contents(MediaType.`application/json`("Reason", ErrorMessage("Message")))
      )
    )
  )

  private lazy val example = json"""{
      "components": {
          "schemas": {},
          "examples": {},
          "securitySchemes": {
              "private-token": {
                  "description": "User's Personal Access Token in GitLab",
                  "name": "PRIVATE-TOKEN",
                  "type": "apiKey",
                  "in": "header"
              },
              "oauth_auth": {
                  "openIdConnectUrl": "/auth/realms/Renku/.well-known/openid-configuration",
                  "type": "openIdConnect"
              }
          }
      },
      "openapi": "3.0.3",
      "info": {
          "title": "Knowledge Graph API",
          "description": "Get info about datasets, users, activities, and other entities",
          "version": "2.15.0-246-g8ac5a0de"
      },
      "servers": [
          {
              "url": "/knowledge-graph",
              "description": "Renku Knowledge Graph API",
              "variables": {}
          }
      ],
      "paths": {
          "/projects/{namespace}/{projectName}/files/{location}/lineage": {
              "get": {
                  "summary": "",
                  "security": [],
                  "parameters": [
                      {
                          "name": "namespace",
                          "in": "path",
                          "description": "Namespace(s) as there might be multiple. Each namespace needs to be url-encoded and separated with a non url-encoded '/'",
                          "required": true,
                          "schema": {
                              "type": "string"
                          }
                      },
                      {
                          "name": "projectName",
                          "in": "path",
                          "description": "Project name",
                          "required": true,
                          "schema": {
                              "type": "string"
                          }
                      },
                      {
                          "name": "location",
                          "in": "path",
                          "description": "The path of the file",
                          "required": true,
                          "schema": {
                              "type": "string"
                          }
                      }
                  ],
                  "responses": {
                      "500": {
                          "description": "Error",
                          "content": {
                              "application/json": {
                                  "examples": {
                                      "Reason": {
                                          "value": {
                                              "message": "Message"
                                          }
                                      }
                                  }
                              }
                          },
                          "links": {},
                          "headers": {}
                      },
                      "404": {
                          "description": "Lineage not found",
                          "content": {
                              "application/json": {
                                  "examples": {
                                      "Reason": {
                                          "value": {
                                              "message": "No lineage for project: namespace/project file: some/file"
                                          }
                                      }
                                  }
                              }
                          },
                          "links": {},
                          "headers": {}
                      },
                      "401": {
                          "description": "Unauthorized",
                          "content": {
                              "application/json": {
                                  "examples": {
                                      "Invalid token": {
                                          "value": {
                                              "message": "Unauthorized"
                                          }
                                      }
                                  }
                              }
                          },
                          "links": {},
                          "headers": {}
                      },
                      "200": {
                          "description": "Lineage found",
                          "content": {
                              "application/json": {
                                  "examples": {
                                      "Sample Lineage": {
                                          "value": {
                                              "edges": [
                                                  {
                                                      "source": "data/zhbikes",
                                                      "target": ".renku/workflow/3144e9a_python.cwl"
                                                  },
                                                  {
                                                      "source": ".renku/workflow/3144e9a_python.cwl",
                                                      "target": "data/preprocessed/zhbikes.parquet"
                                                  }
                                              ],
                                              "nodes": [
                                                  {
                                                      "id": "data/zhbikes",
                                                      "location": "data/zhbikes",
                                                      "label": "data/zhbikes@bbdc429",
                                                      "type": "Directory"
                                                  },
                                                  {
                                                      "id": ".renku/workflow/3144e9a_python.cwl",
                                                      "location": ".renku/workflow/3144e9a_python.cwl",
                                                      "label": "renku run python src/clean_data.py data/zhbikes data/preprocessed/zhbikes.parquet",
                                                      "type": "ProcessRun"
                                                  },
                                                  {
                                                      "id": "data/preprocessed/zhbikes.parquet",
                                                      "location": "data/preprocessed/zhbikes.parquet",
                                                      "label": "data/preprocessed/zhbikes.parquet@1aaf360",
                                                      "type": "File"
                                                  }
                                              ]
                                          }
                                      }
                                  }
                              }
                          },
                          "links": {},
                          "headers": {}
                      }
                  }
              },
              "description": "Finds lineage of the given file",
              "parameters": [
                  {
                      "name": "namespace",
                      "in": "path",
                      "description": "Namespace(s) as there might be multiple. Each namespace needs to be url-encoded and separated with a non url-encoded '/'",
                      "required": true,
                      "schema": {
                          "type": "string"
                      }
                  },
                  {
                      "name": "projectName",
                      "in": "path",
                      "description": "Project name",
                      "required": true,
                      "schema": {
                          "type": "string"
                      }
                  },
                  {
                      "name": "location",
                      "in": "path",
                      "description": "The path of the file",
                      "required": true,
                      "schema": {
                          "type": "string"
                      }
                  }
              ],
              "summary": "Lineage"
          }
      },
      "security": [
          {},
          {
              "oauth_auth": []
          },
          {
              "private-token": []
          }
      ]
  }"""
}

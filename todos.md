x are all subtypes of ProcessPlan still needed?
x *See with Ralf:* verify the type of SuccessCode in RunPlan.
list of exit codes that are considered successful
x *See with Ralf:* are the steps needed in the Association entity? Yes 
x complete the properties of the RunPlan entity with what is given in the ontology
x complete the properties of the CommandParameter entity with what is given in the ontology
x *See with Ralf:* verify how the Prefix is used in the CommandParameters. e.g -c
x create the CommandArgument (Argument) entity as shown in the ontology?
x *See with Ralf:* should hadRole in the Usage be an @id pointing to a CommandParameter/CommandInput? still needed for backward compatibility  
x finish the refactoring of the Usage entity with the CommandInput instead of the FilePath entity
x Person/Organization should be a subtype of Agent -> this would fix the agent heterogeneous type issue
x *See with CLI team:* renku migrate --no-commit still generates several commits.
x upon migration renku log should use the range of commits from the given commit to the latest migration commit.
x on renku migrate do not use the --no-commit. 
x add invalidation into the new model.(Activity)
x use the invalidation in the lineage query.
x refactor jsonld encoders to fit the new mixin pattern as the one used in CommandParameters
x complete the bundles with proper Entity entities :) for e.g in the Input entities.consumes
- run renku log with the flag firing up SHACL validation
- create an issue to simplify sparql query results binding
x add a test to verify if lineage works when created with the new version of renku

x ask Ralf why CommandParameter is an Entity?
x ask Ralf why do we have a cwl file on a RunPlan (that cwl was invalidated) 
- create an issue to fix activity property on a Generation which is already represented on the reverse of the activity
- unify naming for the model
- model clean-up
x remove graphs which does not contain the given location
x and I think having libyaml installed is enough for pyyaml to pick it up, not special procedure to install it needed
- create an issue for fixing reverse to only handle objects instead of list
x isn't that missing members a bug:
"https://swissdatasciencecenter.github.io/renku-ontology#consumes": [
                  {
                    "@type": [
                      "http://www.w3.org/ns/prov#Collection",
                      "http://www.w3.org/ns/prov#Entity",
                      "http://purl.org/wf4ever/wfprov#Artifact"
                    ],
                    "@id": "https://dev.renku.ch/blob/39d571ccea25635aee872c01c6e7a6cfae85ab84/data/zhbikes",
                    "http://www.w3.org/2000/01/rdf-schema#label": [
                      {
                        "@value": "data/zhbikes@39d571ccea25635aee872c01c6e7a6cfae85ab84"
                      }
                    ],
                    "http://schema.org/isPartOf": [
                      {
                        "@type": [
                          "http://www.w3.org/ns/prov#Location",
                          "http://schema.org/Project"
                        ],
                        "@id": "https://localhost/projects/jakub.chrobasik/test%20191104203427",
                        "http://schema.org/dateCreated": [
                          {
                            "@value": "2019-11-04T19:34:32.992000+00:00"
                          }
                        ],
                        "http://schema.org/creator": [
                          {
                            "@type": [
                              "http://www.w3.org/ns/prov#Person",
                              "http://schema.org/Person"
                            ],
                            "@id": "mailto:jakub.chrobasik@epfl.ch",
                            "http://schema.org/email": [
                              {
                                "@value": "jakub.chrobasik@epfl.ch"
                              }
                            ],
                            "http://www.w3.org/2000/01/rdf-schema#label": [
                              {
                                "@value": "Jakub J\u00f3zef Chrobasik"
                              }
                            ],
                            "http://schema.org/name": [
                              {
                                "@value": "Jakub J\u00f3zef Chrobasik"
                              }
                            ]
                          }
                        ],
                        "http://schema.org/name": [
                          {
                            "@value": "test 191104203427"
                          }
                        ],
                        "http://schema.org/dateUpdated": [
                          {
                            "@value": "2019-11-04T19:34:32.992000+00:00"
                          }
                        ],
                        "http://schema.org/schemaVersion": [
                          {
                            "@value": "1"
                          }
                        ]
                      }
                    ],
                    "http://www.w3.org/ns/prov#hadMember": [],
                    "http://www.w3.org/ns/prov#atLocation": [
                      {
                        "@value": "data/zhbikes"
                      }
                    ]
                  }
                ] 
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
- Person/Organization should be a subtype of Agent -> this would fix the agent heterogeneous type issue
x *See with CLI team:* renku migrate --no-commit still generates several commits.
- upon migration renku log should use the range of commits from the given commit to the latest migration commit.
- on renku migrate do not use the --no-commit. 
- add invalidation into the new model.(Activity)
- use the invalidation in the lineage query.
x refactor jsonld encoders to fit the new mixin pattern as the one used in CommandParameters
- complete the bundles with proper Entity entities :) for e.g in the Input entities.consumes
- ask Ralf why CommandParameter is an Entity?
- run renku log with the flag firing up SHACL validation
- create an issue to simplify sparql query results binding 
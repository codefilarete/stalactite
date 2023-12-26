A module that stores poms for the [orm module](../orm) for each supported database.

See submodules and import their poms as dependency to get support for your database.

Design note :
Submodules are packaged as JARs even if they don't contain any sources, packaging as JAR is mandatory to let user project get transitives dependencies from them, else (POM packaging) those submodules would be BOM (Build Of Material) poms which would require to import them as parent pom, and this would be a bit awkward for an ORM to act as a parent of a user project.
# Package for solving aggregate build

The entry point is [AggregateResolver](AggregateResolver.java): it builds a persister for a whole aggregate of
entities.

- Classes suffixed with `MetadataResolver` are the ones that convert the DSL model to some more ready-to-use one for the
persisters engines
- Then `Resolver` suffixed classes configure the writing part (insert, update, delete) of the entity  
- Finally, `Appender` are the ones that graft the resolved metadata to the aggregate selection (read)


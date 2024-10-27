# Package for Spring Derived Queries support in Stalactite
Implementation is inspired by JPA one, hence, you'll find the [CreateQueryLookupStrategy](CreateQueryLookupStrategy.java) and [PartTreeStalactiteQuery](PartTreeStalactiteQuery.java) main classes.

Supported syntax is the same as the one defined by Spring because the parsing is based on Spring's one (see org.springframework.data.repository.query.parser.PartTree class). 
Which means, to summarize (from Spring code), it supports :
- entity selection, method mays starts with "find|read|get|query|search|stream"
- entity deletion, method must start with "delete|remove"
- entity count, method must start with "count"
- entity existence, method must start with "exists"

Obviously, entity selection follows load logic of defined mapping. And deletion also, which means that entities must be loaded before being deleted.
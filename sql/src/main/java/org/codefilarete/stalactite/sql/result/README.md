# Package dedicated to ResultSet handling

## Iterating over them

You can iterate over a ResultSet with [ResultSetIterator](ResultSetIterator.java) or [RowIterator](RowIterator.java). The former is the rawest way to convert a `ResultSet` because you'll be free to implement `convert(..)`. So you may prefer to use [RowIterator](RowIterator.java) because it's still an `Iterator` but converts `ResultSet` rows to [Row](Row.java) which is barely a `Map` of column names and their values, which is more "portable" and has no need of `SQLException` handling.
The way columns are read from `ResultSet` comes from [ResultSetReader](../statement/binder/README.md#predefined-binders) given at construction time.
 
## Converting them to beans

The richest way to convert a `ResultSet` to beans is brought by [WholeResultSetTransformer](WholeResultSetTransformer.java) which will let you create a bean graph.
You may also be interested in [ResultSetRowTransformer](ResultSetRowTransformer.java) for a simpler usage : convert each row of a `ResultSet` to a bean.
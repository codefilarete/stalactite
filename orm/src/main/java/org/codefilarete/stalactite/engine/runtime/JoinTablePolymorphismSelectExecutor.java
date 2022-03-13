package org.codefilarete.stalactite.engine.runtime;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.builder.SQLQueryBuilder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryEase;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.result.RowIterator;

import static org.codefilarete.stalactite.engine.runtime.SecondPhaseRelationLoader.isDefaultValue;

/**
 * @author Guillaume Mary
 */
public class JoinTablePolymorphismSelectExecutor<C, I, T extends Table> implements SelectExecutor<C, I> {
	
	private final Map<Class<? extends C>, Table> tablePerSubEntity;
	private final Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors;
	private final T mainTable;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public JoinTablePolymorphismSelectExecutor(
			Map<Class<? extends C>, Table> tablePerSubEntity,
			Map<Class<? extends C>, SelectExecutor<C, I>> subEntitiesSelectors,
			T mainTable,
			ConnectionProvider connectionProvider,
			Dialect dialect
	) {
		this.tablePerSubEntity = tablePerSubEntity;
		this.subEntitiesSelectors = subEntitiesSelectors;
		this.mainTable = mainTable;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		// 2 possibilities :
		// - execute a request that join all tables and all relations, then give result to transfomer
		//   Pros : one request, simple approach
		//   Cons : one eventually big/complex request, has some drawback on how to create this request (impacts on parent
		//          Persister behavior) and how to build the transformer. In conclusion quite complex
		// - do it in 2+ phases : one request to determine which id matches which type, then ask each sub classes to load
		//   their own type
		//   Pros : suclasses must know common properties/trunk which will be necessary for updates too (to compute 
		//   differences)
		//   Cons : first request not so easy to write. Performance may be lower because of 1+N (one per subclass) database 
		//   requests
		// => option 2 choosen. May be reviewed later, or make this policy configurable.
		
		// Doing this in 2 phases
		// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
		// - call the right subclass joinExecutor with dedicated ids
		
		Column<T, I> primaryKey = (Column<T, I>) Iterables.first(mainTable.getPrimaryKey().getColumns());
		Query query = QueryEase.
				select(primaryKey, primaryKey.getAlias())
				.from(mainTable)
				.where(primaryKey, Operators.in(ids)).getQuery();
		tablePerSubEntity.values().forEach(subTable -> {
			Column subclassPrimaryKey = Iterables.first((Set<Column>) subTable.getPrimaryKey().getColumns());
			query.select(subclassPrimaryKey, subclassPrimaryKey.getAlias());
			query.getFrom().leftOuterJoin(primaryKey, subclassPrimaryKey);
		});
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
		Map<Column, String> aliases = query.getSelectSurrogate().giveColumnAliases();
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
		try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			Map<String, ResultSetReader> readers = new HashMap<>();
			aliases.forEach((c, as) -> readers.put(as, dialect.getColumnBinderRegistry().getBinder(c)));
			
			RowIterator resultSetIterator = new RowIterator(resultSet, readers);
			ColumnedRow columnedRow = new ColumnedRow(aliases::get);
			resultSetIterator.forEachRemaining(row -> {
				
				// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the good one
				Entry<Class<? extends C>, Table> subclassEntityOnRow = Iterables.find(tablePerSubEntity.entrySet(),
						e -> {
							boolean isPKEmpty = true;
							Iterator<Column> columnIt = e.getValue().getPrimaryKey().getColumns().iterator();
							while (isPKEmpty && columnIt.hasNext()) {
								Column column = columnIt.next();
								isPKEmpty = !isDefaultValue(columnedRow.getValue(column, row));
							}
							return isPKEmpty;
						});
				Class<? extends C> entitySubclass = subclassEntityOnRow.getKey();
				
				// adding identifier to subclass ids
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add((I) columnedRow.getValue(primaryKey, row));
			});
		}
		
		List<C> result = new ArrayList<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(subEntitiesSelectors.get(subclass).select(subclassIds)));
		
		return result;
	}
}

package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.ResultSetReader;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.result.RowIterator;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphismSelectExecutor<C, I, T extends Table, D> implements ISelectExecutor<C, I> {
	
	private final Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass;
	private final Column discriminatorColumn;
	private final SingleTablePolymorphism polymorphismPolicy;
	private final T table;
	private final ConnectionProvider connectionProvider;
	private final Dialect dialect;
	
	public SingleTablePolymorphismSelectExecutor(Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass,
													   Column<T, D> discriminatorColumn,
													   SingleTablePolymorphism polymorphismPolicy,
													   T table,
													   ConnectionProvider connectionProvider,
													   Dialect dialect) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.table = table;
		this.connectionProvider = connectionProvider;
		this.dialect = dialect;
		this.discriminatorColumn = discriminatorColumn;
		this.persisterPerSubclass = persisterPerSubclass;
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		// Doing this in 2 phases
		// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
		// - call the right subclass joinExecutor with dedicated ids
		// TODO : (with which listener ?)
		
		Column<T, I> primaryKey = (Column<T, I>) Iterables.first(table.getPrimaryKey().getColumns());
		Set<Column<T, ?>> columns = new HashSet<>(table.getPrimaryKey().getColumns());
		columns.add(discriminatorColumn);
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(QueryEase.
				select(primaryKey, discriminatorColumn)
				.from(table)
				.where(primaryKey, Operators.in(ids)));
		PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
		Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
		try(ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
			ResultSet resultSet = readOperation.execute();
			Map<String, ResultSetReader> aliases = new HashMap<>();
			columns.forEach(c -> aliases.put(c.getName(), dialect.getColumnBinderRegistry().getBinder(c)));
			
			RowIterator resultSetIterator = new RowIterator(resultSet, aliases);
			ColumnedRow columnedRow = new ColumnedRow(Column::getName);
			resultSetIterator.forEachRemaining(row -> {
				D dtype = (D) columnedRow.getValue(discriminatorColumn, row);
				Class<? extends C> entitySubclass = polymorphismPolicy.getClass(dtype);
				// adding identifier to subclass' ids
				idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
						.add((I) columnedRow.getValue(primaryKey, row));
			});
		}
		
		List<C> result = new ArrayList<>();
		idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(persisterPerSubclass.get(subclass).select(subclassIds)));
		
		return result;
	}
}

package org.codefilarete.stalactite.sql.statement;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.codefilarete.stalactite.engine.runtime.DMLExecutor;
import org.codefilarete.stalactite.mapping.Mapping.UpwhereColumn;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderIndex;
import org.codefilarete.tool.Strings;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Sorter;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK;
import static org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_1;
import static org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_10;
import static org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_100;
import static org.codefilarete.stalactite.sql.statement.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_SEPARATOR;

/**
 * Class for DML generation dedicated to {@link DMLExecutor}. Not expected to be used elsewhere.
 *
 * @author Guillaume Mary
 */
public class DMLGenerator {
	
	private static final String AND = " and ";
	
	private static final String EQUAL_SQL_PARAMETER_MARK_AND = " = " + SQL_PARAMETER_MARK + AND;
	
	protected final ParameterBinderIndex<Column, ParameterBinder> columnBinderRegistry;
	
	protected Sorter<Column> columnSorter;
	
	protected final DMLNameProviderFactory dmlNameProviderFactory;
	
	public DMLGenerator(ParameterBinderIndex<Column, ParameterBinder> columnBinderRegistry, Sorter<Column> columnSorter, DMLNameProviderFactory dmlNameProviderFactory) {
		this.columnBinderRegistry = columnBinderRegistry;
		setColumnSorter(columnSorter);
		this.dmlNameProviderFactory = dmlNameProviderFactory;
	}
	
	public ParameterBinderIndex<Column, ParameterBinder> getColumnBinderRegistry() {
		return columnBinderRegistry;
	}
	
	public void setColumnSorter(Sorter<Column> columnSorter) {
		// by default, no sort is made to avoid superfluous time consumption
		this.columnSorter = Objects.preventNull(columnSorter, NoopSorter.INSTANCE);
	}
	
	/**
	 * Applies an alphabetical sorter on {@link Column}s given to DML generating methods.
	 * Made to do steady checks on SQL orders generated in tests but can be used in production.
	 * 
	 * @see CaseSensitiveSorter#INSTANCE
	 */
	public void sortColumnsAlphabetically() {
		this.setColumnSorter(CaseSensitiveSorter.INSTANCE);
	}
	
	/**
	 * Creates a SQL statement order for inserting some data in a table.
	 * Signature is made so that only columns of the same table can be used.
	 *
	 * @param columns columns that must be inserted, at least 1 element
	 * @param <T> table type
	 * @return a (kind of) prepared statement
	 */
	public <T extends Table<T>> ColumnParameterizedSQL<T> buildInsert(Iterable<? extends Column<T, ?>> columns) {
		Iterable<Column> sortedColumns = sort(columns);
		Table table = Iterables.first(sortedColumns).getTable();
		DDLAppender sqlInsert = new DDLAppender(dmlNameProviderFactory.build(Fromable::getAbsoluteName), "insert into ", table, "(");
		sqlInsert.ccat(sortedColumns, ", ");
		sqlInsert.cat(") values (");
		
		Map<Column<T, ?>, int[]> columnToIndex = new HashMap<>();
		Map<Column<T, ?>, ParameterBinder<?>> parameterBinders = new HashMap<>();
		MutableInt positionCounter = new MutableInt(1);
		Iterables.stream(sortedColumns).forEach(column -> {
			if (column.isAutoGenerated()) {
				sqlInsert.cat("default");
				// adding the auto-generated keys to the result is not mandatory for many database vendor, but some,
				// as Oracle, require to specify which column must be returned in generated keys. Hence we add it
				// to the result which makes it accessible through ColumnParameterizedSQL.getColumnIndexes
				// Note that this column doesn't participate to position counter
				columnToIndex.put(column, new int[] {});
			} else {
				sqlInsert.cat(SQL_PARAMETER_MARK);
				columnToIndex.put(column, new int[] { positionCounter.getValue() });
				positionCounter.increment();
				parameterBinders.put(column, columnBinderRegistry.getBinder(column));
			}
			sqlInsert.cat(SQL_PARAMETER_SEPARATOR);
		});
		sqlInsert.cutTail(SQL_PARAMETER_SEPARATOR.length()).cat(")");
		return new ColumnParameterizedSQL<>(sqlInsert.toString(), columnToIndex, parameterBinders);
	}
	
	/**
	 * Creates a SQL statement order for updating some database rows depending on a where clause.
	 * Signature is made so that only columns of the same table can be used.
	 * 
	 * @param columns columns that must be updated, at least 1 element
	 * @param where columns to use for where clause
	 * @param <T> table type
	 * @return a (kind of) prepared statement
	 */
	public <T extends Table<T>> PreparedUpdate<T> buildUpdate(Iterable<? extends Column<T, ?>> columns, Iterable<? extends Column<T, ?>> where) {
		Iterable<Column> sortedColumns = sort(columns);
		Table table = Iterables.first(sortedColumns).getTable();
		DDLAppender sqlUpdate = new DDLAppender(dmlNameProviderFactory.build(Fromable::getAbsoluteName), "update ", table, " set ");
		Map<UpwhereColumn<T>, Integer> upsertIndexes = new HashMap<>(10);
		Map<UpwhereColumn<T>, ParameterBinder> parameterBinders = new HashMap<>();
		int positionCounter = 1;
		for (Column<T, Object> column : sortedColumns) {
			sqlUpdate.cat(column, " = " + SQL_PARAMETER_MARK_1);
			UpwhereColumn<T> upwhereColumn = new UpwhereColumn<>(column, true);
			upsertIndexes.put(upwhereColumn, positionCounter++);
			parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
		}
		sqlUpdate.cutTail(2);
		if (where.iterator().hasNext()) {
			sqlUpdate.cat(" where ");
			for (Column<T, ?> column : where) {
				sqlUpdate.cat(column, EQUAL_SQL_PARAMETER_MARK_AND);
				UpwhereColumn<T> upwhereColumn = new UpwhereColumn<>(column, false);
				upsertIndexes.put(upwhereColumn, positionCounter++);
				parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
			}
			sqlUpdate.cutTail(AND.length());
		}
		return new PreparedUpdate<>(sqlUpdate.toString(), upsertIndexes, parameterBinders);
	}
	
	/**
	 * Creates a SQL statement order for deleting some database rows depending on a where clause.
	 * Signature is made so that only columns of the same table can be used.
	 *
	 * @param table deletion target table
	 * @param where columns to use for where clause
	 * @param <T> table type
	 * @return a (kind of) prepared statement parameterized by {@link Column}
	 */
	public <T extends Table<T>> ColumnParameterizedSQL<T> buildDelete(T table, Iterable<? extends Column<T, ?>> where) {
		DDLAppender sqlDelete = new DDLAppender(dmlNameProviderFactory.build(Fromable::getAbsoluteName), "delete from ", table);
		sqlDelete.cat(" where ");
		ParameterizedWhere<T> parameterizedWhere = appendWhere(sqlDelete, where);
		sqlDelete.cutTail(5);
		return new ColumnParameterizedSQL<>(sqlDelete.toString(), parameterizedWhere.indexesPerColumn, parameterizedWhere.parameterBinders);
	}
	
	/**
	 * Creates a SQL statement order for deleting some database rows by key (with a "in (?, ?, ? ...)")
	 * Signature is made so that only columns of the same table can be used.
	 *
	 * @param table deletion target table
	 * @param keyColumns key columns to use for where clause
	 * @param whereValuesCount number of parameter in where clause (ie number of key values in where)
	 * @param <T> table type
	 * @return a (kind of) prepared statement parameterized by {@link Column}
	 */
	@SuppressWarnings("squid:ForLoopCounterChangedCheck")
	public <T extends Table<T>> ColumnParameterizedSQL<T> buildDeleteByKey(T table, Collection<Column<T, ?>> keyColumns, int whereValuesCount) {
		DDLAppender sqlDelete = new DDLAppender(dmlNameProviderFactory.build(Fromable::getAbsoluteName), "delete from ", table, " where ");
		ParameterizedWhere parameterizedWhere = appendTupledWhere(sqlDelete, keyColumns, whereValuesCount);
		Map<Column<T, ?>, int[]> columnToIndex = parameterizedWhere.getColumnToIndex();
		Map<Column<T, ?>, ParameterBinder<?>> parameterBinders = parameterizedWhere.getParameterBinders();
		return new ColumnParameterizedSQL<>(sqlDelete.toString(), columnToIndex, parameterBinders);
	}
	
	/**
	 * Creates a SQL statement order for selecting some database rows depending on a where clause.
	 * Signature is made so that only columns of the same table can be used.
	 *
	 * @param table selection target table
	 * @param where columns to use for where clause
	 * @param <T> table type
	 * @return a (kind of) prepared statement parameterized by {@link Column}
	 */
	public <T extends Table<T>> ColumnParameterizedSQL<T> buildSelect(T table, Iterable<? extends Column<T, ?>> columns, Iterable<? extends Column<T, ?>> where) {
		Iterable<Column> sortedColumns = sort(columns);
		DDLAppender sqlSelect = new DDLAppender(dmlNameProviderFactory.build(Fromable::getAbsoluteName), "select ");
		sqlSelect.ccat(sortedColumns, ", ");
		sqlSelect.cat(" from ", table);
		ParameterizedWhere<T> parameterizedWhere;
		if (where.iterator().hasNext()) {
			sqlSelect.cat(" where ");
			parameterizedWhere = appendWhere(sqlSelect, where);
			sqlSelect.cutTail(5);
		} else {
			parameterizedWhere = new ParameterizedWhere<>();
		}
		return new ColumnParameterizedSQL<>(sqlSelect.toString(), parameterizedWhere.indexesPerColumn, parameterizedWhere.parameterBinders);
	}
	
	/**
	 * Creates a SQL statement order for selecting some database rows by key (with a "in (?, ?, ? ...)")
	 * Signature is made so that only columns of the same table can be used.
	 *
	 * @param table selection target table
	 * @param keyColumns key columns to use for where clause
	 * @param whereValuesCount number of parameter in where clause (ie number of key values in where)
	 * @param <T> table type
	 * @return a (kind of) prepared statement parameterized by {@link Column}
	 */
	public <T extends Table<T>> ColumnParameterizedSelect<T> buildSelectByKey(T table, Iterable<? extends Column<T, ?>> columns, Collection<Column<T, ?>> keyColumns, int whereValuesCount) {
		Iterable<Column> sortedColumns = sort(columns);
		DMLNameProvider dmlNameProvider = dmlNameProviderFactory.build(Fromable::getAbsoluteName);
		DDLAppender sqlSelect = new DDLAppender(dmlNameProvider, "select ");
		Map<String, ParameterBinder<?>> selectParameterBinders = new HashMap<>();
		for (Column column : sortedColumns) {
			sqlSelect.cat(column, ", ");
			selectParameterBinders.put(dmlNameProvider.getSimpleName(column), columnBinderRegistry.getBinder(column));
		}
		sqlSelect.cutTail(2).cat(" from ", table, " where ");
		ParameterizedWhere parameterizedWhere = appendTupledWhere(sqlSelect, keyColumns, whereValuesCount);
		Map<Column<T, ?>, int[]> columnToIndex = parameterizedWhere.getColumnToIndex();
		Map<Column<T, ?>, ParameterBinder<?>> parameterBinders = parameterizedWhere.getParameterBinders();
		return new ColumnParameterizedSelect<>(sqlSelect.toString(), columnToIndex, parameterBinders, selectParameterBinders);
	}
	
	protected Iterable<Column> sort(Iterable<? extends Column> columns) {
		return this.columnSorter.sort(columns);
	}
	
	public static class NoopSorter implements Sorter<Column> {
		
		public static final NoopSorter INSTANCE = new NoopSorter();
		
		@Override
		public Iterable<Column> sort(Iterable<? extends Column> columns) {
			return (Iterable<Column>) columns;
		}
	}
	
	/**
	 * Sorts columns passed as arguments.
	 * Optional action since it's used in appending String operation.
	 * Useful for unit tests and debug operations : give steady (and logical) column places in SQL, without this, order
	 * is given by Column hashcode and HashMap algorithm.
	 */
	public static class CaseSensitiveSorter<C extends Column> implements Sorter<C> {
		
		public static final CaseSensitiveSorter INSTANCE = new CaseSensitiveSorter();
		
		@Override
		public Iterable<C> sort(Iterable<? extends C> columns) {
			TreeSet<C> result = new TreeSet<>(ColumnNameComparator.INSTANCE);
			for (C column : columns) {
				result.add(column);
			}
			return result;
		}
	}
	
	/**
	 * Appends a where condition (without "where" keyword) to a given sql order.
	 * Conditions are appended with the form of ands such as {@code a = ? and b = ? and ...}
	 *
	 * @param sql the sql order on which to append the clause
	 * @param conditionColumns columns of the where
	 * @param <T> type of the table
	 * @return an object that contains indexes and parameter binders of the where
	 */
	private <T extends Table> ParameterizedWhere<T> appendWhere(DDLAppender sql, Iterable<? extends Column<T, ?>> conditionColumns) {
		ParameterizedWhere<T> result = new ParameterizedWhere<>();
		MutableInt positionCounter = new MutableInt(1);
		Iterables.stream(conditionColumns).forEach(column -> {
			sql.cat(column, EQUAL_SQL_PARAMETER_MARK_AND);
			result.indexesPerColumn.put(column, new int[] { positionCounter.getValue() });
			positionCounter.increment();
			result.parameterBinders.put(column, columnBinderRegistry.getBinder(column));
		});
		return result;
	}
	
	/**
	 * Appends a where condition (without "where" keyword) to a given sql order.
	 * Conditions are appended with the form of tuples such as {@code (a, b) in ((?, ?), (?, ?), ...)}
	 * 
	 * @param sql the sql order on which to append the clause
	 * @param conditionColumns columns of the where
	 * @param whereValuesCount expected number of tuples
	 * @param <T> type of the table
	 * @return an object that contains indexes and parameter binders of the where
	 */
	public <T extends Table> ParameterizedWhere<T> appendTupledWhere(DDLAppender sql, Collection<Column<T, ?>> conditionColumns, int whereValuesCount) {
		ParameterizedWhere<T> result = new ParameterizedWhere<>();
		boolean isComposedKey = conditionColumns.size() > 1;
		sql.catIf(isComposedKey, "(")
				.ccat(conditionColumns, ", ")
				.catIf(isComposedKey, ")");
		sql.cat(" in (");
		StringBuilder repeat = Strings.repeat(conditionColumns.size(), SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
		repeat.setLength(repeat.length() - 2);
		String keyMarks = repeat.toString();
		for (int i = 1; i <= whereValuesCount; i++) {
			sql.catIf(isComposedKey, "(").cat(keyMarks);
			sql.catIf(isComposedKey, ")").cat(", ");
			// because statement indexes start at 0, we must decrement index of 1
			int startKeyMarkIndex = i-1;
			MutableInt pkIndex = new MutableInt();
			conditionColumns.forEach(keyColumn -> {
				int pkColumnIndex = startKeyMarkIndex * conditionColumns.size() + pkIndex.increment();
				result.indexesPerColumn.computeIfAbsent(keyColumn, k -> new int[whereValuesCount])[startKeyMarkIndex] = pkColumnIndex;
			});
		}
		conditionColumns.forEach(keyColumn -> result.parameterBinders.put(keyColumn, columnBinderRegistry.getBinder(keyColumn)));
		sql.cutTail(2).cat(")");
		return result;
	}
	
	public static class ParameterizedWhere<T extends Table> {
		
		private final Map<Column<T, ?>, int[]> indexesPerColumn = new HashMap<>();
		
		private final Map<Column<T, ?>, ParameterBinder<?>> parameterBinders = new HashMap<>();
		
		public Map<Column<T, ?>, int[]> getColumnToIndex() {
			return indexesPerColumn;
		}
		
		public Map<Column<T, ?>, ParameterBinder<?>> getParameterBinders() {
			return parameterBinders;
		}
		
	}
	
	private static class ColumnNameComparator implements Comparator<Column> {
		
		private static final ColumnNameComparator INSTANCE = new ColumnNameComparator();
		
		@Override
		public int compare(Column o1, Column o2) {
			return String.CASE_INSENSITIVE_ORDER.compare(o1.getAbsoluteName(), o2.getAbsoluteName());
		}
	}
}
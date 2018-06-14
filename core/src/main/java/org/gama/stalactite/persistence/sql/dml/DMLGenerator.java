package org.gama.stalactite.persistence.sql.dml;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.ISorter;
import org.gama.lang.collection.Iterables;
import org.gama.lang.trace.ModifiableInt;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.engine.DMLExecutor;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;

import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK;
import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_1;
import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_10;
import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.SQL_PARAMETER_MARK_100;

/**
 * Class for DML generation dedicated to {@link DMLExecutor}. Not expected to be used elsewhere.
 *
 * @author Guillaume Mary
 */
public class DMLGenerator {
	
	private static final String EQUAL_SQL_PARAMETER_MARK_AND = " = " + SQL_PARAMETER_MARK + " and ";
	
	private final ColumnBinderRegistry columnBinderRegistry;
	
	private final ISorter<Iterable<? extends Column>> columnSorter;
	
	private final DMLNameProvider dmlNameProvider;
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry) {
		this(columnBinderRegistry, NoopSorter.INSTANCE);
	}
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry, ISorter<Iterable<? extends Column>> columnSorter) {
		this(columnBinderRegistry, columnSorter, new DMLNameProvider(Collections.emptyMap()));
	}
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry, ISorter<Iterable<? extends Column>> columnSorter, DMLNameProvider dmlNameProvider) {
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnSorter = columnSorter;
		this.dmlNameProvider = dmlNameProvider;
	}
	
	public <T extends Table> ColumnParamedSQL<T> buildInsert(Iterable<? extends Column<T, Object>> columns) {
		columns = (Iterable<? extends Column<T, Object>>) sort(columns);
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlInsert = new StringAppender("insert into ", dmlNameProvider.getSimpleName(table), "(");
		dmlNameProvider.catWithComma(columns, sqlInsert);
		sqlInsert.cat(") values (");
		
		Map<Column, int[]> columnToIndex = new HashMap<>();
		Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		ModifiableInt positionCounter = new ModifiableInt(1);
		Iterables.stream(columns).forEach(column -> {
			sqlInsert.cat(SQL_PARAMETER_MARK_1);
			columnToIndex.put(column, new int[] { positionCounter.getValue() });
			positionCounter.increment();
			parameterBinders.put(column, columnBinderRegistry.getBinder(column));
		});
		sqlInsert.cutTail(2).cat(")");
		return new ColumnParamedSQL(sqlInsert.toString(), columnToIndex, parameterBinders);
	}
	
	public <T extends Table> PreparedUpdate<T> buildUpdate(Iterable<? extends Column<T, Object>> columns, Iterable<? extends Column<T, Object>> where) {
		columns = (Iterable<? extends Column<T, Object>>) sort(columns);
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlUpdate = new StringAppender("update ", dmlNameProvider.getSimpleName(table), " set ");
		Map<UpwhereColumn<T>, Integer> upsertIndexes = new HashMap<>(10);
		Map<UpwhereColumn<T>, ParameterBinder> parameterBinders = new HashMap<>();
		int positionCounter = 1;
		for (Column<T, Object> column : columns) {
			sqlUpdate.cat(dmlNameProvider.getSimpleName(column), " = " + SQL_PARAMETER_MARK_1);
			UpwhereColumn<T> upwhereColumn = new UpwhereColumn<>(column, true);
			upsertIndexes.put(upwhereColumn, positionCounter++);
			parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
		}
		sqlUpdate.cutTail(2).cat(" where ");
		for (Column<T, Object> column : where) {
			sqlUpdate.cat(dmlNameProvider.getSimpleName(column), EQUAL_SQL_PARAMETER_MARK_AND);
			UpwhereColumn<T> upwhereColumn = new UpwhereColumn<>(column, false);
			upsertIndexes.put(upwhereColumn, positionCounter++);
			parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
		}
		return new PreparedUpdate<T>(sqlUpdate.cutTail(5).toString(), upsertIndexes, parameterBinders);
	}
	
	public <T extends Table> ColumnParamedSQL<T> buildDelete(T table, Iterable<? extends Column<T, Object>> where) {
		StringAppender sqlDelete = new StringAppender("delete from ", dmlNameProvider.getSimpleName(table));
		sqlDelete.cat(" where ");
		Map<Column<T, Object>, int[]> columnToIndex = new HashMap<>();
		Map<Column<T, Object>, ParameterBinder> parameterBinders = new HashMap<>();
		appendWhere(sqlDelete, where, columnToIndex, parameterBinders);
		sqlDelete.cutTail(5);
		return new ColumnParamedSQL<>(sqlDelete.toString(), columnToIndex, parameterBinders);
	}
	
	private <T extends Table> void appendWhere(StringAppender sql,
												  Iterable<? extends Column<T, Object>> where,
												  Map<Column<T, Object>, int[]> columnToIndex,
												  Map<Column<T, Object>, ParameterBinder> parameterBinders) {
		ModifiableInt positionCounter = new ModifiableInt(1);
		Iterables.stream(where).forEach(column -> {
			sql.cat(dmlNameProvider.getSimpleName(column), EQUAL_SQL_PARAMETER_MARK_AND);
			columnToIndex.put(column, new int[] { positionCounter.getValue() });
			positionCounter.increment();
			parameterBinders.put(column, columnBinderRegistry.getBinder(column));
		});
	}
	
	@SuppressWarnings("squid:ForLoopCounterChangedCheck")
	public <T extends Table> ColumnParamedSQL<T> buildMassiveDelete(T table, Column<T, Object> keyColumn, int whereValuesCount) {
		StringAppender sqlDelete = new StringAppender("delete from ", dmlNameProvider.getSimpleName(table));
		sqlDelete.cat(" where ", dmlNameProvider.getSimpleName(keyColumn), " in (");
		Strings.repeat(sqlDelete.getAppender(), whereValuesCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
		sqlDelete.cutTail(2).cat(")");
		Map<Column<T, Object>, int[]> columnToIndex = new HashMap<>();
		Map<Column<T, Object>, ParameterBinder> parameterBinders = new HashMap<>();
		int[] indexes = new int[whereValuesCount];
		for (int i = 0; i < whereValuesCount;) {
			indexes[i] = ++i;
		}
		columnToIndex.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, columnBinderRegistry.getBinder(keyColumn));
		return new ColumnParamedSQL<>(sqlDelete.toString(), columnToIndex, parameterBinders);
	}
	
	public <T extends Table> ColumnParamedSQL<T> buildSelect(T table, Iterable<? extends Column<T, Object>> columns, Iterable<? extends Column<T, Object>> where) {
		columns = (Iterable<? extends Column<T, Object>>) sort(columns);
		StringAppender sqlSelect = new StringAppender("select ");
		dmlNameProvider.catWithComma(columns, sqlSelect);
		sqlSelect.cat(" from ", dmlNameProvider.getSimpleName(table), " where ");
		Map<Column<T, Object>, int[]> columnToIndex = new HashMap<>();
		Map<Column<T, Object>, ParameterBinder> parameterBinders = new HashMap<>();
		appendWhere(sqlSelect, where, columnToIndex, parameterBinders);
		sqlSelect.cutTail(5);
		return new ColumnParamedSQL<>(sqlSelect.toString(), columnToIndex, parameterBinders);
	}
	
	public <T extends Table> ColumnParamedSelect<T> buildMassiveSelect(T table, Iterable<? extends Column<T, Object>> columns, Column<T, Object> keyColumn, int whereValuesCount) {
		columns = (Iterable<? extends Column<T, Object>>) sort(columns);
		StringAppender sqlSelect = new StringAppender("select ");
		Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
		for (Column column : columns) {
			String selectedColumnName = dmlNameProvider.getSimpleName(column);
			sqlSelect.cat(selectedColumnName, ", ");
			selectParameterBinders.put(selectedColumnName, columnBinderRegistry.getBinder(column));
		}
		sqlSelect.cutTail(2).cat(" from ", dmlNameProvider.getSimpleName(table), " where ", dmlNameProvider.getSimpleName(keyColumn), " in (");
		Strings.repeat(sqlSelect.getAppender(), whereValuesCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
		sqlSelect.cutTail(2).cat(")");
		Map<Column<T, Object>, int[]> columnToIndex = new HashMap<>();
		Map<Column<T, Object>, ParameterBinder> parameterBinders = new HashMap<>();
		int[] indexes = new int[whereValuesCount];
		for (int i = 0; i < whereValuesCount; i++) {
			indexes[i] = i + 1;
		}
		columnToIndex.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, columnBinderRegistry.getBinder(keyColumn));
		return new ColumnParamedSelect<>(sqlSelect.toString(), columnToIndex, parameterBinders, selectParameterBinders);
	}
	
	private Iterable<? extends Column> sort(Iterable<? extends Column> columns) {
		return this.columnSorter.sort(columns);
	}
	
	public static class NoopSorter implements ISorter<Iterable<? extends Column>> {
		
		public static final NoopSorter INSTANCE = new NoopSorter();
		
		@Override
		public Iterable<? extends Column> sort(Iterable<? extends Column> columns) {
			return columns;
		}
	}
	
	/**
	 * Sorts columns passed as arguments.
	 * Optional action since it's used in appending String operation.
	 * Usefull for unit tests and debug operations : give steady (and logical) column places in SQL, without this, order
	 * is given by Column hashcode and HashMap algorithm.
	 */
	public static class CaseSensitiveSorter<C extends Column> implements ISorter<Iterable<C>> {
		
		public static final CaseSensitiveSorter INSTANCE = new CaseSensitiveSorter();
		
		@Override
		public Iterable<C> sort(Iterable<C> columns) {
			TreeSet<C> result = new TreeSet<>(ColumnNameComparator.INSTANCE);
			for (C column : columns) {
				result.add(column);
			}
			return result;
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

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
import org.gama.lang.trace.IncrementableInt;
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
	
	private final ISorter<Iterable<Column>> columnSorter;
	
	private final DMLNameProvider dmlNameProvider;
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry) {
		this(columnBinderRegistry, NoopSorter.INSTANCE);
	}
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry, ISorter<Iterable<Column>> columnSorter) {
		this(columnBinderRegistry, columnSorter, new DMLNameProvider(Collections.emptyMap()));
	}
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry, ISorter<Iterable<Column>> columnSorter, DMLNameProvider dmlNameProvider) {
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnSorter = columnSorter;
		this.dmlNameProvider = dmlNameProvider;
	}
	
	public ColumnParamedSQL buildInsert(Iterable<Column> columns) {
		columns = sort(columns);
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlInsert = new StringAppender("insert into ", dmlNameProvider.getSimpleName(table), "(");
		dmlNameProvider.catWithComma(columns, sqlInsert);
		sqlInsert.cat(") values (");
		
		Map<Column, int[]> columnToIndex = new HashMap<>();
		Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		IncrementableInt positionCounter = new IncrementableInt(1);
		Iterables.stream(columns).forEach(column -> {
			sqlInsert.cat(SQL_PARAMETER_MARK_1);
			columnToIndex.put(column, new int[] { positionCounter.getValue() });
			positionCounter.increment();
			parameterBinders.put(column, columnBinderRegistry.getBinder(column));
		});
		sqlInsert.cutTail(2).cat(")");
		return new ColumnParamedSQL(sqlInsert.toString(), columnToIndex, parameterBinders);
	}
	
	public PreparedUpdate buildUpdate(Iterable<Column> columns, Iterable<Column> where) {
		columns = sort(columns);
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlUpdate = new StringAppender("update ", dmlNameProvider.getSimpleName(table), " set ");
		Map<UpwhereColumn, Integer> upsertIndexes = new HashMap<>(10);
		Map<UpwhereColumn, ParameterBinder> parameterBinders = new HashMap<>();
		int positionCounter = 1;
		for (Column column : columns) {
			sqlUpdate.cat(dmlNameProvider.getSimpleName(column), " = " + SQL_PARAMETER_MARK_1);
			UpwhereColumn upwhereColumn = new UpwhereColumn(column, true);
			upsertIndexes.put(upwhereColumn, positionCounter++);
			parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
		}
		sqlUpdate.cutTail(2).cat(" where ");
		for (Column column : where) {
			sqlUpdate.cat(dmlNameProvider.getSimpleName(column), EQUAL_SQL_PARAMETER_MARK_AND);
			UpwhereColumn upwhereColumn = new UpwhereColumn(column, false);
			upsertIndexes.put(upwhereColumn, positionCounter++);
			parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
		}
		return new PreparedUpdate(sqlUpdate.cutTail(5).toString(), upsertIndexes, parameterBinders);
	}
	
	public ColumnParamedSQL buildDelete(Table table, Iterable<Column> where) {
		StringAppender sqlDelete = new StringAppender("delete from ", dmlNameProvider.getSimpleName(table));
		sqlDelete.cat(" where ");
		Map<Column, int[]> columnToIndex = new HashMap<>();
		Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		appendWhere(sqlDelete, where, columnToIndex, parameterBinders);
		sqlDelete.cutTail(5);
		return new ColumnParamedSQL(sqlDelete.toString(), columnToIndex, parameterBinders);
	}
	
	private void appendWhere(StringAppender sql, Iterable<Column> where, Map<Column, int[]> columnToIndex, Map<Column, ParameterBinder> 
			parameterBinders) {
		IncrementableInt positionCounter = new IncrementableInt(1);
		Iterables.stream(where).forEach(column -> {
			sql.cat(dmlNameProvider.getSimpleName(column), EQUAL_SQL_PARAMETER_MARK_AND);
			columnToIndex.put(column, new int[] { positionCounter.getValue() });
			positionCounter.increment();
			parameterBinders.put(column, columnBinderRegistry.getBinder(column));
		});
	}
	
	@SuppressWarnings("squid:ForLoopCounterChangedCheck")
	public ColumnParamedSQL buildMassiveDelete(Table table, Column keyColumn, int whereValuesCount) {
		StringAppender sqlDelete = new StringAppender("delete from ", dmlNameProvider.getSimpleName(table));
		sqlDelete.cat(" where ", dmlNameProvider.getSimpleName(keyColumn), " in (");
		Strings.repeat(sqlDelete.getAppender(), whereValuesCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
		sqlDelete.cutTail(2).cat(")");
		Map<Column, int[]> columnToIndex = new HashMap<>();
		Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		int[] indexes = new int[whereValuesCount];
		for (int i = 0; i < whereValuesCount;) {
			indexes[i] = ++i;
		}
		columnToIndex.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, columnBinderRegistry.getBinder(keyColumn));
		return new ColumnParamedSQL(sqlDelete.toString(), columnToIndex, parameterBinders);
	}
	
	public ColumnParamedSQL buildSelect(Table table, Iterable<Column> columns, Iterable<Column> where) {
		columns = sort(columns);
		StringAppender sqlSelect = new StringAppender("select ");
		dmlNameProvider.catWithComma(columns, sqlSelect);
		sqlSelect.cat(" from ", dmlNameProvider.getSimpleName(table), " where ");
		Map<Column, int[]> columnToIndex = new HashMap<>();
		Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		appendWhere(sqlSelect, where, columnToIndex, parameterBinders);
		sqlSelect.cutTail(5);
		return new ColumnParamedSQL(sqlSelect.toString(), columnToIndex, parameterBinders);
	}
	
	public ColumnParamedSelect buildMassiveSelect(Table table, Iterable<Column> columns, Column keyColumn, int whereValuesCount) {
		columns = sort(columns);
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
		Map<Column, int[]> columnToIndex = new HashMap<>();
		Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		int[] indexes = new int[whereValuesCount];
		for (int i = 0; i < whereValuesCount; i++) {
			indexes[i] = i + 1;
		}
		columnToIndex.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, columnBinderRegistry.getBinder(keyColumn));
		return new ColumnParamedSelect(sqlSelect.toString(), columnToIndex, parameterBinders, selectParameterBinders);
	}
	
	private Iterable<Column> sort(Iterable<Column> columns) {
		return this.columnSorter.sort(columns);
	}
	
	public static class NoopSorter implements ISorter<Iterable<Column>> {
		
		public static final NoopSorter INSTANCE = new NoopSorter();
		
		@Override
		public Iterable<Column> sort(Iterable<Column> columns) {
			return columns;
		}
	}
	
	/**
	 * Sorts columns passed as arguments.
	 * Optional action since it's used in appending String operation.
	 * Usefull for unit tests and debug operations : give steady (and logical) column places in SQL, without this, order
	 * is given by Column hashcode and HashMap algorithm.
	 */
	public static class CaseSensitiveSorter implements ISorter<Iterable<Column>> {
		
		public static final CaseSensitiveSorter INSTANCE = new CaseSensitiveSorter();
		
		@Override
		public Iterable<Column> sort(Iterable<Column> columns) {
			TreeSet<Column> result = new TreeSet<>(ColumnNameComparator.INSTANCE);
			for (Column column : columns) {
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

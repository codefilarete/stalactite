package org.gama.stalactite.persistence.sql.dml;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.ISorter;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.engine.PersisterExecutor;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.sql.dml.PreparedUpdate.UpwhereColumn;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.gama.sql.dml.ExpandableSQL.ExpandableParameter.*;

/**
 * Class for DML generation dedicated to {@link PersisterExecutor}. Not expected to be used elsewhere.
 * 
 * @author Guillaume Mary
 */
public class DMLGenerator {
	
	/** Comparator used to have  */
	public static final ColumnNameComparator COLUMN_NAME_COMPARATOR = new ColumnNameComparator();
	
	private final ColumnBinderRegistry columnBinderRegistry;
	
	private final ISorter<Iterable<Column>> columnSorter;
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry) {
		this(columnBinderRegistry, NoopSorter.INSTANCE);
	}
	
	public DMLGenerator(ColumnBinderRegistry columnBinderRegistry, ISorter<Iterable<Table.Column>> columnSorter) {
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnSorter = columnSorter;
	}
	
	public ColumnPreparedSQL buildInsert(Iterable<Column> columns) {
		columns = sort(columns);
		Table table = Iterables.first(columns).getTable();
		final StringAppender sqlInsert = new StringAppender("insert into ", table.getName(), "(");
		DDLTableGenerator.catWithComma(columns, sqlInsert);
		sqlInsert.cat(") values (");
		
		final Map<Column, int[]> columnToIndex = new HashMap<>();
		final Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		Iterables.visit(columns, new ForEach<Column, Object>() {
			private int positionCounter = 1;
			@Override
			public Object visit(Column column) {
				sqlInsert.cat(SQL_PARAMETER_MARK_1);
				columnToIndex.put(column, new int[] { positionCounter++ });
				parameterBinders.put(column, columnBinderRegistry.getBinder(column));
				// return value doesn't matter
				return null;
			}
		});
		sqlInsert.cutTail(2).cat(")");
		return new ColumnPreparedSQL(sqlInsert.toString(), columnToIndex, parameterBinders);
	}
	
	public PreparedUpdate buildUpdate(Iterable<Column> columns, Iterable<Column> where) {
		columns = sort(columns);
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlUpdate = new StringAppender("update ", table.getName(), " set ");
		Map<UpwhereColumn, Integer> upsertIndexes = new HashMap<>(10);
		Map<UpwhereColumn, ParameterBinder> parameterBinders = new HashMap<>();
		int positionCounter = 1;
		for (Column column : columns) {
			sqlUpdate.cat(column.getName(), " = ?, ");
			UpwhereColumn upwhereColumn = new UpwhereColumn(column, true);
			upsertIndexes.put(upwhereColumn, positionCounter++);
			parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
		}
		sqlUpdate.cutTail(2).cat(" where ");
		for (Column column : where) {
			sqlUpdate.cat(column.getName(), " = ? and ");
			UpwhereColumn upwhereColumn = new UpwhereColumn(column, false);
			upsertIndexes.put(upwhereColumn, positionCounter++);
			parameterBinders.put(upwhereColumn, columnBinderRegistry.getBinder(column));
		}
		return new PreparedUpdate(sqlUpdate.cutTail(5).toString(), upsertIndexes, parameterBinders);
	}
	
	public ColumnPreparedSQL buildDelete(Table table, final Iterable<Column> where) {
		final StringAppender sqlDelete = new StringAppender("delete from ", table.getName());
		sqlDelete.cat(" where ");
		final Map<Column, int[]> columnToIndex = new HashMap<>();
		final Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		Iterables.visit(where, new ForEach<Column, Object>() {
			private int positionCounter = 1;
			
			@Override
			public Object visit(Column column) {
				sqlDelete.cat(column.getName(), " = ? and ");
				columnToIndex.put(column, new int[]{positionCounter++});
				parameterBinders.put(column, columnBinderRegistry.getBinder(column));
				// return value doesn't matter
				return null;
			}
		});
		sqlDelete.cutTail(5);
		return new ColumnPreparedSQL(sqlDelete.toString(), columnToIndex, parameterBinders);
	}
	
	public ColumnPreparedSQL buildMassiveDelete(Table table, Column keyColumn, int whereValuesCount) {
		StringAppender sqlDelete = new StringAppender("delete from ", table.getName());
		sqlDelete.cat(" where ", keyColumn.getName(), " in (");
		Strings.repeat(sqlDelete.getAppender(), whereValuesCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
		sqlDelete.cutTail(2).cat(")");
		final Map<Column, int[]> columnToIndex = new HashMap<>();
		final Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		int[] indexes = new int[whereValuesCount];
		for (int i = 0; i < whereValuesCount; i++) {
			indexes[i] = i+1;
		}
		columnToIndex.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, columnBinderRegistry.getBinder(keyColumn));
		return new ColumnPreparedSQL(sqlDelete.toString(), columnToIndex, parameterBinders);
	}
	
	public ColumnPreparedSQL buildSelect(Table table, Iterable<Column> columns, Iterable<Column> where) {
		columns = sort(columns);
		final StringAppender sqlSelect = new StringAppender("select ");
		DDLTableGenerator.catWithComma(columns, sqlSelect);
		sqlSelect.cat(" from ", table.getName(), " where ");
		final Map<Column, int[]> columnToIndex = new HashMap<>();
		final Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		Iterables.visit(where, new ForEach<Column, Object>() {
			private int positionCounter = 1;
			
			@Override
			public Object visit(Column column) {
				sqlSelect.cat(column.getName(), " = ? and ");
				columnToIndex.put(column, new int[]{positionCounter++});
				parameterBinders.put(column, columnBinderRegistry.getBinder(column));
				// return value doesn't matter
				return null;
			}
		});
		sqlSelect.cutTail(5);
		return new ColumnPreparedSQL(sqlSelect.toString(), columnToIndex, parameterBinders);
	}
	
	public PreparedSelect buildMassiveSelect(Table table, Iterable<Column> columns, Column keyColumn, int whereValuesCount) {
		columns = sort(columns);
		StringAppender sqlSelect = new StringAppender("select ");
		Map<String, ParameterBinder> selectParameterBinders = new HashMap<>();
		for (Column column : columns) {
			String selectedColumnName = column.getName();
			sqlSelect.cat(selectedColumnName, ", ");
			selectParameterBinders.put(selectedColumnName, columnBinderRegistry.getBinder(column));
		}
		sqlSelect.cutTail(2).cat(" from ", table.getName(), " where ", keyColumn.getName(), " in (");
		Strings.repeat(sqlSelect.getAppender(), whereValuesCount, SQL_PARAMETER_MARK_1, SQL_PARAMETER_MARK_100, SQL_PARAMETER_MARK_10);
		sqlSelect.cutTail(2).cat(")");
		Map<Column, int[]> columnToIndex = new HashMap<>();
		Map<Column, ParameterBinder> parameterBinders = new HashMap<>();
		int[] indexes = new int[whereValuesCount];
		for (int i = 0; i < whereValuesCount; i++) {
			indexes[i] = i+1;
		}
		columnToIndex.put(keyColumn, indexes);
		parameterBinders.put(keyColumn, columnBinderRegistry.getBinder(keyColumn));
		return new PreparedSelect(sqlSelect.toString(), columnToIndex, parameterBinders, selectParameterBinders);
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
	 * Sort columns passed as arguments.
	 * Optional action since it's used in appending String operation.
	 * Usefull for unit tests and debug operations : give steady (and logical) column places in SQL, without this, order
	 * is given by Column hashcode and HashMap algorithm.
	 */
	public static class CaseSensitiveSorter implements ISorter<Iterable<Column>> {
		
		public static final NoopSorter INSTANCE = new NoopSorter();
		
		@Override
		public Iterable<Column> sort(Iterable<Column> columns) {
			TreeSet<Column> result = new TreeSet<>(COLUMN_NAME_COMPARATOR);
			for (Column column : columns) {
				result.add(column);
			}
			return result;
		}
	}
	
	private static class ColumnNameComparator implements Comparator<Column> {
		@Override
		public int compare(Column o1, Column o2) {
			return String.CASE_INSENSITIVE_ORDER.compare(o1.getAbsoluteName(), o2.getAbsoluteName());
		}
	}
}

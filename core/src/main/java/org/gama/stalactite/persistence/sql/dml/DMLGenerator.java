package org.gama.stalactite.persistence.sql.dml;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.sql.ddl.DDLTableGenerator;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * Class for DML generation.
 * No rocket science here.
 * 
 * @author Guillaume Mary
 */
public class DMLGenerator {
	
	/** Comparator used to have  */
	public static final ColumnNameComparator COLUMN_NAME_COMPARATOR = new ColumnNameComparator();
	
	public InsertOperation buildInsert(Iterable<Column> columns) {
		columns = sort(columns);
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlInsert = new StringAppender("insert into ", table.getName(), "(");
		DDLTableGenerator.catWithComma(columns, sqlInsert);
		sqlInsert.cat(") values (");
		Map<Column, Integer> upsertIndexes = new HashMap<>(10);
		int positionCounter = 1;
		for (Column column : columns) {
			sqlInsert.cat("?, ");
			upsertIndexes.put(column, positionCounter++);
		}
		String sql = sqlInsert.cutTail(2).cat(")").toString();
		return new InsertOperation(sql, upsertIndexes);
	}
	
	public UpdateOperation buildUpdate(Iterable<Column> columns, Iterable<Column> where) {
		columns = sort(columns);
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlUpdate = new StringAppender("update ", table.getName(), " set ");
		Map<Column, Integer> upsertIndexes = new HashMap<>(10);
		int positionCounter = 1;
		for (Column column : columns) {
			sqlUpdate.cat(column.getName(), " = ?, ");
			upsertIndexes.put(column, positionCounter++);
		}
		sqlUpdate.cutTail(2);
		Map<Column, Integer> whereIndexes = catWhere(where, sqlUpdate, positionCounter);
		String sql = sqlUpdate.toString();
		return new UpdateOperation(sql, upsertIndexes, whereIndexes);
	}
	
	public DeleteOperation buildDelete(Table table, Iterable<Column> where) {
		StringAppender sqlDelete = new StringAppender("delete ", table.getName());
		Map<Column, Integer> whereIndexes = catWhere(where, sqlDelete);
		return new DeleteOperation(sqlDelete.toString(), whereIndexes);
	}
	
	public SelectOperation buildSelect(Table table, Iterable<Column> columns, Iterable<Column> where) {
		columns = sort(columns);
		StringAppender sqlSelect = new StringAppender("select ");
		DDLTableGenerator.catWithComma(columns, sqlSelect);
		sqlSelect.cat(" from ", table.getName());
		Map<Column, Integer> whereIndexes = catWhere(where, sqlSelect);
		return new SelectOperation(sqlSelect.toString(), whereIndexes, Iterables.copy(columns));
	}
	
	/**
	 * Sort columns passed as arguments.
	 * Optional action since it's used in appending String operation.
	 * Usefull for unit tests and debug operations : give steady (and logical) column places in SQL, without this, order
	 * is given by Column hashcode and HashMap algorithm.
	 *
	 * @param columns
	 * @return
	 */
	protected Iterable<Column> sort(Iterable<Column> columns) {
		TreeSet<Column> sorter = new TreeSet<>(COLUMN_NAME_COMPARATOR);
		for (Column column : columns) {
			sorter.add(column);
		}
		return sorter;
	}
	
	private Map<Column, Integer> catWhere(Iterable<Column> where, StringAppender sql) {
		return catWhere(where, sql, 1);
	}
	
	private Map<Column, Integer> catWhere(Iterable<Column> where, StringAppender sql, int positionCounter) {
		Map<Column, Integer> colToIndexes = new HashMap<>(2, 1);
		if (where.iterator().hasNext()) {
			sql.cat(" where ");
			for (Column column : where) {
				sql.cat(column.getName(), " = ? and ");
				colToIndexes.put(column, positionCounter++);
			}
			sql.cutTail(5);
		}
		return colToIndexes;
	}
	
	private static class ColumnNameComparator implements Comparator<Column> {
		@Override
		public int compare(Column o1, Column o2) {
			return String.CASE_INSENSITIVE_ORDER.compare(o1.getAbsoluteName(), o2.getAbsoluteName());
		}
	}
}

package org.stalactite.persistence.sql.dml;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.stalactite.lang.StringAppender;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class DMLGenerator {
	
	public CRUDStatement buildInsert(Iterable<Column> columns) {
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlInsert = new StringAppender("insert into ", table.getName(), "(");
		DDLGenerator.catWithComma(columns, sqlInsert);
		sqlInsert.cat(") values (");
		Map<Column, Integer> upsertIndexes = new HashMap<>(10);
		int positionCounter = 1;
		for (Column column : columns) {
			sqlInsert.cat("?, ");
			upsertIndexes.put(column, positionCounter++);
		}
		String sql = sqlInsert.cutTail(2).cat(")").toString();
		return new CRUDStatement(upsertIndexes, sql);
	}

	public CRUDStatement buildUpdate(Iterable<Column> columns, Iterable<Column> where) {
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlUpdate = new StringAppender("update ", table.getName(), " set ");
		Map<Column, Integer> upsertIndexes = new HashMap<>(10);
		int positionCounter = 1;
		for (Column column : columns) {
			sqlUpdate.cat(column.getName(), " = ?, ");
			upsertIndexes.put(column, positionCounter++);
		}
		sqlUpdate.cutTail(2);
		Map<Column, Integer> whereIndexes = catWhere(where, sqlUpdate);
		String sql = sqlUpdate.toString();
		return new CRUDStatement(upsertIndexes, sql, whereIndexes);
	}
	
	public CRUDStatement buildDelete(Table table, Iterable<Column> where) {
		StringAppender sqlDelete = new StringAppender("delete ", table.getName());
		Map<Column, Integer> whereIndexes = catWhere(where, sqlDelete);
		return new CRUDStatement(sqlDelete.toString(), whereIndexes);
	}
	
	public CRUDStatement buildSelect(Table table, List<Column> columns, Iterable<Column> where) {
		StringAppender sqlSelect = new StringAppender("select ");
		for (Column column : columns) {
			sqlSelect.cat(column.getName(), ", ");
		}
		sqlSelect.cutTail(2).cat(" from ", table.getName());
		Map<Column, Integer> whereIndexes = catWhere(where, sqlSelect);
		return new CRUDStatement(sqlSelect.toString(), whereIndexes);
	}
	
	private Map<Column, Integer> catWhere(Iterable<Column> where, StringAppender sql) {
		Map<Column, Integer> colToIndexes = new HashMap<>(2, 1);
		if (where.iterator().hasNext()) {
			sql.cat(" where ");
			int positionCounter = 1;
			for (Column column : where) {
				sql.cat(column.getName(), " = ? and ");
				colToIndexes.put(column, positionCounter++);
			}
			sql.cutTail(5);
		}
		return colToIndexes;
	}
}

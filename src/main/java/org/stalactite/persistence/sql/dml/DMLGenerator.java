package org.stalactite.persistence.sql.dml;

import org.stalactite.lang.StringAppender;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.persistence.sql.ddl.DDLGenerator;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class DMLGenerator {
	
	public String buildInsert(Iterable<Column> columns) {
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlInsert = new StringAppender("insert into ", table.getName(), "(");
		DDLGenerator.catWithComma(columns, sqlInsert);
		sqlInsert.cat(") values (");
		for (Column column : columns) {
			sqlInsert.cat("?, ");
		}
		return sqlInsert.cutTail(2).cat(")").toString();
	}

	public String buildUpdate(Iterable<Column> columns, Iterable<Column> where) {
		Table table = Iterables.first(columns).getTable();
		StringAppender sqlUpdate = new StringAppender("update ", table.getName(), " set ");
		for (Column column : columns) {
			sqlUpdate.cat(column.getName(), " = ?, ");
		}
		sqlUpdate.cutTail(2);
		catWhere(where, sqlUpdate);
		return sqlUpdate.toString();
	}
	
	public String buildDelete(Table table, Iterable<Column> where) {
		StringAppender sqlDelete = new StringAppender("delete ", table.getName());
		catWhere(where, sqlDelete);
		return sqlDelete.toString();
	}
	
	private void catWhere(Iterable<Column> where, StringAppender sql) {
		if (where.iterator().hasNext()) {
			sql.cat(" where ");
			for (Column column : where) {
				sql.cat(column.getName(), " = ? and ");
			}
			sql.cutTail(5);
		}
	}
}

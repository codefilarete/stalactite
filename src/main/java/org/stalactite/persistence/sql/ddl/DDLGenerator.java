package org.stalactite.persistence.sql.ddl;

import org.stalactite.lang.StringAppender;
import org.stalactite.lang.collection.Iterables;
import org.stalactite.lang.collection.Iterables.ForEach;
import org.stalactite.persistence.structure.ForeignKey;
import org.stalactite.persistence.structure.Index;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class DDLGenerator {
	
	public static final ForEach<Column, String> FOREACH_COLUMNNAME = new ForEachColumnName();
	
	private final JavaTypeToSqlTypeMapping dialect;
	
	public DDLGenerator(JavaTypeToSqlTypeMapping dialect) {
		this.dialect = dialect;
	}

	public String generateCreateTable(Table table) {
		StringAppender sqlCreateTable = new StringAppender("create table ", table.getName(), "(");
		for (Column column : table.getColumns()) {
			sqlCreateTable.cat(column.getName(), " ", getSqlType(column));
			if (column.isPrimaryKey()) {
				sqlCreateTable.cat(" primary key");
			} else if (!column.isNullable()) {
				sqlCreateTable.cat(" not null");
			}
			sqlCreateTable.cat(", ");
		}
		sqlCreateTable.cutTail(2).cat(")");
//			sqlCreateTable.cat(" ROW_FORMAT=COMPRESSED");
		return sqlCreateTable.toString();
	}

	protected String getSqlType(Column column) {
		return dialect.getTypeName(column);
	}

	public String generateCreateIndex(Index index) {
		Table table = index.getTargetTable();
		StringAppender sqlCreateIndex = new StringAppender("create")
				.catIf(index.isUnique(), " unique")
				.cat(" index ", index.getName(), " on ", table.getName(), "(");
		catWithComma(index.getColumns(), sqlCreateIndex);
		return sqlCreateIndex.cat(")").toString();
	}

	public String generateCreateForeignKey(ForeignKey foreignKey) {
		Table table = foreignKey.getTargetTable();
		StringAppender sqlCreateFK = new StringAppender("alter table ", table.getName())
				.cat(" add constraint ", foreignKey.getName(), " foreign key(");
		catWithComma(foreignKey.getColumns(), sqlCreateFK);
		sqlCreateFK.cat(") references ", foreignKey.getTargetTable().getName(), "(");
		catWithComma(foreignKey.getTargetColumns(), sqlCreateFK);
		return sqlCreateFK.cat(")").toString();
	}
	
	public static void catWithComma(Iterable<Column> targetColumns, StringAppender sqlCreateFK) {
		cat(sqlCreateFK, targetColumns, FOREACH_COLUMNNAME).cutTail(2);
	}
	
	public static class ForEachColumnName extends ForEach<Column, String> {
		@Override
		public String visit(Column column) {
			return column.getName() + ", ";
		}
	}
	
	public static <O> StringAppender cat(final StringAppender appender, Iterable<O> iterable, final ForEach<O, String> mapper) {
		Iterables.visit(iterable, new ForEach<O, Object>() {
			
			@Override
			public Void visit(O o) {
				appender.cat(mapper.visit(o));
				return null;
			}
		});
		return appender;
	}
	
}

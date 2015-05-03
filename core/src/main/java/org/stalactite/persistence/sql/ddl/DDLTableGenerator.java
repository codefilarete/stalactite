package org.stalactite.persistence.sql.ddl;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Iterables.ForEach;
import org.stalactite.persistence.structure.Table.ForeignKey;
import org.stalactite.persistence.structure.Table.Index;
import org.stalactite.persistence.structure.Table;
import org.stalactite.persistence.structure.Table.Column;

/**
 * @author mary
 */
public class DDLTableGenerator {
	
	public static final ForEach<Column, String> FOREACH_COLUMNNAME = new ForEachColumnName();
	
	private final JavaTypeToSqlTypeMapping typeMapping;
	
	public DDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping) {
		this.typeMapping = typeMapping;
	}

	public String generateCreateTable(Table table) {
		StringAppender sqlCreateTable = new StringAppender("create table ", table.getName(), "(");
		for (Column column : table.getColumns()) {
			sqlCreateTable.cat(column.getName(), " ", getSqlType(column));
			sqlCreateTable.catIf(!column.isNullable(), " not null").cat(", ");
		}
		sqlCreateTable.cutTail(2);
		if (table.getPrimaryKey() != null) {
			sqlCreateTable.cat(", primary key (", table.getPrimaryKey().getName(), ")");
		}
		sqlCreateTable.cat(")");
//			sqlCreateTable.cat(" ROW_FORMAT=COMPRESSED");
		return sqlCreateTable.toString();
	}

	protected String getSqlType(Column column) {
		return typeMapping.getTypeName(column);
	}

	public String generateCreateIndex(Index index) {
		Table table = index.getTable();
		StringAppender sqlCreateIndex = new StringAppender("create")
				.catIf(index.isUnique(), " unique")
				.cat(" index ", index.getName(), " on ", table.getName(), "(");
		catWithComma(index.getColumns(), sqlCreateIndex);
		return sqlCreateIndex.cat(")").toString();
	}

	public String generateCreateForeignKey(ForeignKey foreignKey) {
		Table table = foreignKey.getTable();
		StringAppender sqlCreateFK = new StringAppender("alter table ", table.getName())
				.cat(" add constraint ", foreignKey.getName(), " foreign key(");
		catWithComma(foreignKey.getColumns(), sqlCreateFK);
		sqlCreateFK.cat(") references ", foreignKey.getTargetTable().getName(), "(");
		catWithComma(foreignKey.getTargetColumns(), sqlCreateFK);
		return sqlCreateFK.cat(")").toString();
	}
	
	public String generateDropTable(Table table) {
		StringAppender sqlCreateTable = new StringAppender("drop table if exists ", table.getName());
		return sqlCreateTable.toString();
	}
	
	public static void catWithComma(Iterable<Column> targetColumns, StringAppender sql) {
		cat(sql, targetColumns, FOREACH_COLUMNNAME).cutTail(2);
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

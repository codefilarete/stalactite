package org.gama.stalactite.persistence.sql.ddl;

import java.util.function.Function;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public class DDLTableGenerator {
	
	private final JavaTypeToSqlTypeMapping typeMapping;
	
	public DDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping) {
		this.typeMapping = typeMapping;
	}

	public String generateCreateTable(Table table) {
		StringAppender sqlCreateTable = new StringAppender("create table ", table.getAbsoluteName(), "(");
		for (Column column : table.getColumns()) {
			generateCreateColumn(column, sqlCreateTable);
			sqlCreateTable.cat(", ");
		}
		sqlCreateTable.cutTail(2);
		if (table.getPrimaryKey() != null) {
			generateCreatePrimaryKey(table, sqlCreateTable);
		}
		sqlCreateTable.cat(")");
		return sqlCreateTable.toString();
	}
	
	protected void generateCreatePrimaryKey(Table table, StringAppender sqlCreateTable) {
		sqlCreateTable.cat(", primary key (", table.getPrimaryKey().getName(), ")");
	}
	
	protected void generateCreateColumn(Column column, StringAppender sqlCreateTable) {
		sqlCreateTable.cat(column.getName(), " ", getSqlType(column))
				.catIf(!column.isNullable(), " not null");
		
	}
	
	protected String getSqlType(Column column) {
		return typeMapping.getTypeName(column);
	}

	public String generateCreateIndex(Index index) {
		Table table = index.getTable();
		StringAppender sqlCreateIndex = new StringAppender("create")
				.catIf(index.isUnique(), " unique")
				.cat(" index ", index.getName(), " on ", table.getAbsoluteName(), "(");
		catWithComma(index.getColumns(), sqlCreateIndex);
		return sqlCreateIndex.cat(")").toString();
	}
	
	public String generateCreateForeignKey(ForeignKey foreignKey) {
		Table table = foreignKey.getTable();
		StringAppender sqlCreateFK = new StringAppender("alter table ", table.getAbsoluteName())
				.cat(" add constraint ", foreignKey.getName(), " foreign key(");
		catWithComma(foreignKey.getColumns(), sqlCreateFK);
		sqlCreateFK.cat(") references ", foreignKey.getTargetTable().getName(), "(");
		catWithComma(foreignKey.getTargetColumns(), sqlCreateFK);
		return sqlCreateFK.cat(")").toString();
	}
	
	public String generateAddColumn(Column column) {
		StringAppender sqladdColumn = new StringAppender("alter table ", column.getTable().getAbsoluteName(),
				" add column ", column.getName(), " ", getSqlType(column));
		return sqladdColumn.toString();
	}
	
	public String generateDropTable(Table table) {
		StringAppender sqlCreateTable = new StringAppender("drop table if exists ", table.getAbsoluteName());
		return sqlCreateTable.toString();
	}
	
	public String generateDropIndex(Index index) {
		StringAppender sqlCreateTable = new StringAppender("drop index ", index.getName());
		return sqlCreateTable.toString();
	}
	
	public String generateDropForeignKey(ForeignKey foreignKey) {
		StringAppender sqlCreateTable = new StringAppender("alter table ", foreignKey.getTable().getAbsoluteName(),
				" drop constraint ", foreignKey.getName());
		return sqlCreateTable.toString();
	}
	
	public String generateDropColumn(Column column) {
		StringAppender sqlDropColumn = new StringAppender("alter table ", column.getTable().getAbsoluteName(),
				" drop column ", column.getName());
		return sqlDropColumn.toString();
	}
	
	public static void catWithComma(Iterable<Column> targetColumns, StringAppender sql) {
		cat(sql, targetColumns, column -> column.getName() + ", ").cutTail(2);
	}
	
	public static <O> StringAppender cat(StringAppender appender, Iterable<O> iterable, Function<O, String> mapper) {
		Iterables.stream(iterable).forEach(o -> appender.cat(mapper.apply(o)));
		return appender;
	}
	
}

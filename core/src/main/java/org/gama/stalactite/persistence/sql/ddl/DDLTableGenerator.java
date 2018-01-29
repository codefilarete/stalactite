package org.gama.stalactite.persistence.sql.ddl;

import java.util.Collections;

import org.gama.lang.StringAppender;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.DMLNameProvider;

/**
 * @author Guillaume Mary
 */
public class DDLTableGenerator {
	
	private final JavaTypeToSqlTypeMapping typeMapping;
	
	protected final DMLNameProvider dmlNameProvider;
	
	public DDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping) {
		this(typeMapping, new DMLNameProvider(Collections.emptyMap()));
	}

	public DDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping, DMLNameProvider dmlNameProvider) {
		this.typeMapping = typeMapping;
		this.dmlNameProvider = dmlNameProvider;
	}
	
	public String generateCreateTable(Table table) {
		StringAppender sqlCreateTable = new StringAppender("create table ", dmlNameProvider.getSimpleName(table), "(");
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
		sqlCreateTable.cat(", primary key (", dmlNameProvider.getSimpleName(table.getPrimaryKey()), ")");
	}
	
	protected void generateCreateColumn(Column column, StringAppender sqlCreateTable) {
		sqlCreateTable.cat(dmlNameProvider.getSimpleName(column), " ", getSqlType(column))
				.catIf(!column.isNullable(), " not null");
		
	}
	
	protected String getSqlType(Column column) {
		return typeMapping.getTypeName(column);
	}

	public String generateCreateIndex(Index index) {
		Table table = index.getTable();
		StringAppender sqlCreateIndex = new StringAppender("create")
				.catIf(index.isUnique(), " unique")
				.cat(" index ", index.getName(), " on ", dmlNameProvider.getSimpleName(table), "(");
		dmlNameProvider.catWithComma(index.getColumns(), sqlCreateIndex);
		return sqlCreateIndex.cat(")").toString();
	}
	
	public String generateCreateForeignKey(ForeignKey foreignKey) {
		Table table = foreignKey.getTable();
		StringAppender sqlCreateFK = new StringAppender("alter table ", dmlNameProvider.getSimpleName(table))
				.cat(" add constraint ", foreignKey.getName(), " foreign key(");
		dmlNameProvider.catWithComma(foreignKey.getColumns(), sqlCreateFK);
		sqlCreateFK.cat(") references ", dmlNameProvider.getSimpleName(foreignKey.getTargetTable()), "(");
		dmlNameProvider.catWithComma(foreignKey.getTargetColumns(), sqlCreateFK);
		return sqlCreateFK.cat(")").toString();
	}
	
	public String generateAddColumn(Column column) {
		StringAppender sqladdColumn = new StringAppender("alter table ", dmlNameProvider.getSimpleName(column.getTable()),
				" add column ", dmlNameProvider.getSimpleName(column), " ", getSqlType(column));
		return sqladdColumn.toString();
	}
	
	public String generateDropTable(Table table) {
		StringAppender sqlCreateTable = new StringAppender("drop table ", dmlNameProvider.getSimpleName(table));
		return sqlCreateTable.toString();
	}
	
	public String generateDropTableIfExists(Table table) {
		StringAppender sqlCreateTable = new StringAppender("drop table if exists ", dmlNameProvider.getSimpleName(table));
		return sqlCreateTable.toString();
	}
	
	public String generateDropIndex(Index index) {
		StringAppender sqlCreateTable = new StringAppender("drop index ", index.getName());
		return sqlCreateTable.toString();
	}
	
	public String generateDropForeignKey(ForeignKey foreignKey) {
		StringAppender sqlCreateTable = new StringAppender("alter table ", dmlNameProvider.getSimpleName(foreignKey.getTable()),
				" drop constraint ", foreignKey.getName());
		return sqlCreateTable.toString();
	}
	
	public String generateDropColumn(Column column) {
		StringAppender sqlDropColumn = new StringAppender("alter table ", dmlNameProvider.getSimpleName(column.getTable()),
				" drop column ", dmlNameProvider.getSimpleName(column));
		return sqlDropColumn.toString();
	}
	
}

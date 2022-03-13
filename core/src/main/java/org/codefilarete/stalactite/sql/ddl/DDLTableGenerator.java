package org.codefilarete.stalactite.sql.ddl;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Index;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class DDLTableGenerator {
	
	private final SqlTypeRegistry typeMapping;
	
	protected final DMLNameProvider dmlNameProvider;
	
	public DDLTableGenerator(SqlTypeRegistry typeMapping) {
		this(typeMapping, new DMLNameProvider(Collections.emptyMap()));
	}

	public DDLTableGenerator(SqlTypeRegistry typeMapping, DMLNameProvider dmlNameProvider) {
		this.typeMapping = typeMapping;
		this.dmlNameProvider = dmlNameProvider;
	}
	
	public String generateCreateTable(Table table) {
		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "create table ", table, "(");
		for (Column column : (Set<Column>) table.getColumns()) {
			generateCreateColumn(column, sqlCreateTable);
			sqlCreateTable.cat(", ");
		}
		sqlCreateTable.cutTail(2);
		if (table.getPrimaryKey() != null) {
			generateCreatePrimaryKey(table.getPrimaryKey(), sqlCreateTable);
		}
		sqlCreateTable.cat(")");
		return sqlCreateTable.toString();
	}
	
	protected void generateCreatePrimaryKey(@Nonnull PrimaryKey primaryKey, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(", primary key (")
			.ccat(primaryKey.getColumns(), ", ")
			.cat(")");
	}
	
	protected void generateCreateColumn(Column column, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(column, " ", getSqlType(column))
				.catIf(!column.isNullable(), " not null");
		
	}
	
	protected String getSqlType(Column column) {
		return typeMapping.getTypeName(column);
	}

	public String generateCreateIndex(Index index) {
		Table table = index.getTable();
		StringAppender sqlCreateIndex = new DDLAppender(dmlNameProvider, "create")
				.catIf(index.isUnique(), " unique")
				.cat(" index ", index.getName(), " on ", table, "(")
				.ccat(index.getColumns(), ", ");
		return sqlCreateIndex.cat(")").toString();
	}
	
	public String generateCreateForeignKey(ForeignKey foreignKey) {
		Table table = foreignKey.getTable();
		StringAppender sqlCreateFK = new DDLAppender(dmlNameProvider, "alter table ", table)
				.cat(" add constraint ", foreignKey.getName(), " foreign key(")
				.ccat(foreignKey.getColumns(), ", ")
				.cat(") references ", foreignKey.getTargetTable(), "(")
				.ccat(foreignKey.getTargetColumns(), ", ");
		return sqlCreateFK.cat(")").toString();
	}
	
	public String generateAddColumn(Column column) {
		DDLAppender sqladdColumn = new DDLAppender(dmlNameProvider, "alter table ", column.getTable(), " add column ", column, " ", getSqlType(column));
		return sqladdColumn.toString();
	}
	
	public String generateDropTable(Table table) {
		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "drop table ", table);
		return sqlCreateTable.toString();
	}
	
	public String generateDropTableIfExists(Table table) {
		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "drop table if exists ", table);
		return sqlCreateTable.toString();
	}
	
	public String generateDropIndex(Index index) {
		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "drop index ", index.getName());
		return sqlCreateTable.toString();
	}
	
	public String generateDropForeignKey(ForeignKey foreignKey) {
		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "alter table ", foreignKey.getTable(), " drop constraint ", foreignKey.getName());
		return sqlCreateTable.toString();
	}
	
	public String generateDropColumn(Column column) {
		DDLAppender sqlDropColumn = new DDLAppender(dmlNameProvider, "alter table ", column.getTable(), " drop column ", column);
		return sqlDropColumn.toString();
	}
	
}

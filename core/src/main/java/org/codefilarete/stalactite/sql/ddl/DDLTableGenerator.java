package org.codefilarete.stalactite.sql.ddl;

import java.util.HashMap;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Index;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.ddl.structure.UniqueConstraint;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class DDLTableGenerator {
	
	private final SqlTypeRegistry typeMapping;
	
	protected final DMLNameProvider dmlNameProvider;
	
	private DDLTableGenerator(SqlTypeRegistry typeMapping, DMLNameProvider dmlNameProvider) {
		this.typeMapping = typeMapping;
		this.dmlNameProvider = dmlNameProvider;
	}

	public DDLTableGenerator(SqlTypeRegistry typeMapping, DMLNameProviderFactory dmlNameProviderFactory) {
		this(typeMapping, dmlNameProviderFactory.build(new HashMap<>()));
	}
	
	public String generateCreateTable(Table<?> table) {
		DDLAppender sqlCreateTable = new DDLAppender(dmlNameProvider, "create table ", table, "(");
		for (Column<?, ?> column : table.getColumns()) {
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
	
	protected void generateCreatePrimaryKey(PrimaryKey<?, ?> primaryKey, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(", primary key (")
			.ccat(primaryKey.getColumns(), ", ")
			.cat(")");
	}
	
	protected void generateCreateColumn(Column<?, ?> column, DDLAppender sqlCreateTable) {
		sqlCreateTable.cat(column, " ", getSqlType(column))
				.catIf(Boolean.FALSE.equals(column.getNullable()), " not null");
	}
	
	protected String getSqlType(Column<?, ?> column) {
		return typeMapping.getTypeName(column);
	}
	
	public String generateCreateIndex(Index<?> index) {
		Table<?> table = index.getTable();
		StringAppender sqlCreateIndex = new DDLAppender(dmlNameProvider, "create")
				.catIf(index.isUnique(), " unique")
				.cat(" index ", index.getName(), " on ", table, "(")
				.ccat(index.getColumns(), ", ");
		return sqlCreateIndex.cat(")").toString();
	}
	
	public String generateCreateForeignKey(ForeignKey<?, ?, ?> foreignKey) {
		Table<?> table = foreignKey.getTable();
		StringAppender sqlCreateFK = new DDLAppender(dmlNameProvider, "alter table ", table)
				.cat(" add constraint ", foreignKey.getName(), " foreign key(")
				.ccat(foreignKey.getColumns(), ", ")
				.cat(") references ", foreignKey.getTargetTable(), "(")
				.ccat(foreignKey.getTargetColumns(), ", ");
		return sqlCreateFK.cat(")").toString();
	}
	
	public String generateCreateUniqueConstraint(UniqueConstraint uniqueConstraint) {
		Table<?> table = uniqueConstraint.getTable();
		StringAppender sqlCreateIndex = new DDLAppender(dmlNameProvider, "alter table ")
				.cat(table, " add constraint ", uniqueConstraint.getName(), " unique ", "(")
				.ccat(uniqueConstraint.getColumns(), ", ");
		return sqlCreateIndex.cat(")").toString();
	}
	
	public String generateAddColumn(Column<?, ?> column) {
		DDLAppender addColumnStatement = new DDLAppender(dmlNameProvider, "alter table ", column.getTable(), " add column ", column, " ", getSqlType(column));
		return addColumnStatement.toString();
	}
	
	public String generateDropTable(Table<?> table) {
		DDLAppender dropTableStatement = new DDLAppender(dmlNameProvider, "drop table ", table);
		return dropTableStatement.toString();
	}
	
	public String generateDropTableIfExists(Table<?> table) {
		DDLAppender dropTableStatement = new DDLAppender(dmlNameProvider, "drop table if exists ", table);
		return dropTableStatement.toString();
	}
	
	public String generateDropIndex(Index<?> index) {
		DDLAppender dropIndexStatement = new DDLAppender(dmlNameProvider, "drop index ", index.getName());
		return dropIndexStatement.toString();
	}
	
	public String generateDropForeignKey(ForeignKey<?, ?, ?> foreignKey) {
		DDLAppender dropConstraintStatement = new DDLAppender(dmlNameProvider, "alter table ", foreignKey.getTable(), " drop constraint ", foreignKey.getName());
		return dropConstraintStatement.toString();
	}
	
	public String generateDropColumn(Column<?, ?> column) {
		DDLAppender dropColumnStatement = new DDLAppender(dmlNameProvider, "alter table ", column.getTable(), " drop column ", column);
		return dropColumnStatement.toString();
	}
}

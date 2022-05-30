package org.codefilarete.stalactite.sql;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.sql.ddl.DDLAppender;
import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.SqlTypeRegistry;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.H2TypeMapping;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class H2Dialect extends Dialect { 
	
	public H2Dialect() {
		super(new H2TypeMapping());
	}
	
	@Override
	protected H2DDLTableGenerator newDdlTableGenerator() {
		return new H2DDLTableGenerator(getSqlTypeRegistry());
	}
	
	public static class H2DDLTableGenerator extends DDLTableGenerator {
		
		public H2DDLTableGenerator(SqlTypeRegistry typeMapping) {
			super(typeMapping, new H2DMLNameProvier(Collections.emptyMap()));
		}
		
		@Override
		protected String getSqlType(Column column) {
			String sqlType = super.getSqlType(column);
			if (column.isAutoGenerated()) {
				sqlType += " GENERATED ALWAYS AS IDENTITY";
			}
			return sqlType;
		}
		
		/** Overridden to implement H2 "unique" keyword */
		@Override
		protected void generateCreatePrimaryKey(PrimaryKey primaryKey, DDLAppender sqlCreateTable) {
			sqlCreateTable.cat(", unique (")
					.ccat(primaryKey.getColumns(), ", ")
					.cat(")");
		}
	}
	
	
	public static class H2DMLNameProvier extends DMLNameProvider {
		
		/** H2 keywords to be escape. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER));
		
		public H2DMLNameProvier(Map<Table, String> tableAliases) {
			super(tableAliases);
		}
		
		@Override
		public String getSimpleName(@Nonnull Column column) {
			if (KEYWORDS.contains(column.getName())) {
				return "`" + column.getName() + "`";
			}
			return super.getSimpleName(column);
		}
		
		@Override
		public String getSimpleName(Fromable table) {
			if (KEYWORDS.contains(table.getName())) {
				return "`" + super.getSimpleName(table) + "`";
			}
			return super.getSimpleName(table);
		}
	}
	
}

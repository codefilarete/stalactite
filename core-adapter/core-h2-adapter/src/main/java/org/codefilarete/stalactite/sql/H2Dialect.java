package org.codefilarete.stalactite.sql;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.H2DDLTableGenerator;
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
	
	
	public static class H2DMLNameProvider extends DMLNameProvider {
		
		/** H2 keywords to be escape. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER));
		
		public H2DMLNameProvider(Map<Table, String> tableAliases) {
			super(tableAliases);
		}
		
		@Override
		public String getSimpleName(@Nonnull Selectable<?> column) {
			if (KEYWORDS.contains(column.getExpression())) {
				return "`" + column.getExpression() + "`";
			}
			return super.getSimpleName(column);
		}
		
		@Override
		public String getName(Fromable table) {
			if (KEYWORDS.contains(table.getName())) {
				return "`" + super.getName(table) + "`";
			}
			return super.getName(table);
		}
	}
	
}

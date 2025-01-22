package org.codefilarete.stalactite.sql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.H2DDLTableGenerator;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.H2ParameterBinderRegistry;
import org.codefilarete.stalactite.sql.statement.binder.H2TypeMapping;
import org.codefilarete.tool.collection.Arrays;

/**
 * @author Guillaume Mary
 */
public class H2Dialect extends DefaultDialect { 
	
	public H2Dialect() {
		super(new H2TypeMapping(), new H2ParameterBinderRegistry());
	}
	
	@Override
	protected DMLNameProviderFactory newDMLNameProviderFactory() {
		return H2DMLNameProvider::new;
	}
	
	@Override
	protected H2DDLTableGenerator newDdlTableGenerator() {
		return new H2DDLTableGenerator(getSqlTypeRegistry(), H2DMLNameProvider::new);
	}
	
	
	public static class H2DMLNameProvider extends DMLNameProvider {
		
		/** H2 keywords to be escape. TODO: to be completed */
		public static final Set<String> KEYWORDS = Collections.unmodifiableSet(Arrays.asTreeSet(String.CASE_INSENSITIVE_ORDER));
		
		public H2DMLNameProvider(Function<Fromable, String> tableAliaser) {
			super(tableAliaser);
		}
		
		public H2DMLNameProvider(Map<Table, String> tableAliases) {
			super(tableAliases);
		}
		
		@Override
		public String getSimpleName(Selectable<?> column) {
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

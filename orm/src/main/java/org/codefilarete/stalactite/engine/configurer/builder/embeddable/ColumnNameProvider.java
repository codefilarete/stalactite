package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Wrapper to give {@link Column} name according to given {@link ColumnNamingStrategy} if present.
 * If absent {@link ColumnNamingStrategy#DEFAULT} will be used.
 */
public class ColumnNameProvider {
	
	private final ColumnNamingStrategy columnNamingStrategy;
	
	public ColumnNameProvider(@Nullable ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = nullable(columnNamingStrategy).getOr(ColumnNamingStrategy.DEFAULT);
	}
	
	public String giveColumnName(ColumnLinkage linkage) {
		return nullable(linkage.getColumnName())
				.elseSet(nullable(linkage.getField()).map(Field::getName))
				.getOr(() -> giveColumnName(linkage.getAccessorDefinition()));
	}
	
	protected String giveColumnName(AccessorDefinition accessorDefinition) {
		return columnNamingStrategy.giveName(accessorDefinition);
	}

	public static class ColumnLinkage {
		
		@Nullable
		private final String columnName;

		@Nullable
		private final Field field;
		
		private final ReversibleAccessor<?, ?> accessor;
		
		private final AccessorDefinition accessorDefinition;

		public ColumnLinkage(@Nullable String columnName, @Nullable Field field, ReversibleAccessor<?, ?> accessor) {
			this.columnName = columnName;
			this.field = field;
			this.accessor = accessor;
			this.accessorDefinition = AccessorDefinition.giveDefinition(accessor);
		}

		@Nullable
		public String getColumnName() {
			return columnName;
		}
		
		@Nullable
		public Field getField() {
			return field;
		}

		public ReversibleAccessor<?, ?> getAccessor() {
			return accessor;
		}

		public AccessorDefinition getAccessorDefinition() {
			return accessorDefinition;
		}
	}
}

package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

import org.codefilarete.reflection.AccessorDefinition;
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
	
	protected String giveColumnName(EmbeddableLinkage linkage) {
		return nullable(linkage.getColumnName())
				.elseSet(nullable(linkage.getField()).map(Field::getName))
				.getOr(() -> giveColumnName(AccessorDefinition.giveDefinition(linkage.getAccessor())));
	}
	
	protected String giveColumnName(AccessorDefinition accessorDefinition) {
		return columnNamingStrategy.giveName(accessorDefinition);
	}
}

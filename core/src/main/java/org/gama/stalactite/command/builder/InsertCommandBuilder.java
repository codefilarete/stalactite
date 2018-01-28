package org.gama.stalactite.command.builder;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.StringAppender;
import org.gama.lang.Strings;
import org.gama.lang.collection.Iterables;
import org.gama.lang.trace.IncrementableInt;
import org.gama.sql.binder.ParameterBinderIndex;
import org.gama.stalactite.command.model.Insert;
import org.gama.stalactite.persistence.sql.dml.ColumnParamedSQL;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.SQLBuilder;

/**
 * A SQL builder for {@link Insert} objects
 * 
 * @author Guillaume Mary
 */
public class InsertCommandBuilder implements SQLBuilder {
	
	private final Insert insert;
	
	public InsertCommandBuilder(Insert insert) {
		this.insert = insert;
	}
	
	@Override
	public String toSQL() {
		return new StringAppender("insert into ")
				.cat(insert.getTargetTable().getAbsoluteName())
				.cat("(").ccat(Iterables.collectToList(insert.getColumns(), Column::getName), ", ").cat(")")
				.cat(" values (").cat(Strings.repeat(insert.getColumns().size(), "?, ")).cutTail(2).cat(")")
				.toString();
	}
	
	public ColumnParamedSQL toStatement(ParameterBinderIndex<Column> parameterBinderIndex) {
		Map<Column, int[]> columnIndexes = new HashMap<>();
		IncrementableInt indexCount = new IncrementableInt();	// prepared statement indexes starts at 1
		insert.getColumns().forEach(c -> 
			columnIndexes.put(c, new int[] { indexCount.increment() })
		);
		return new ColumnParamedSQL(toSQL(), columnIndexes, parameterBinderIndex);
	}
}

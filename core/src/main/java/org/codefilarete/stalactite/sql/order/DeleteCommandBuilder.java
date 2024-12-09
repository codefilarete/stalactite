package org.codefilarete.stalactite.sql.order;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.ExpandableSQLAppender;
import org.codefilarete.stalactite.query.builder.PreparableSQLBuilder;
import org.codefilarete.stalactite.query.builder.SQLAppender;
import org.codefilarete.stalactite.query.builder.SQLBuilder;
import org.codefilarete.stalactite.query.builder.StringSQLAppender;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;

/**
 * A SQL builder for {@link Delete} objects
 * Can hardly be shared with {@link DMLGenerator} because the latter doesn't handle multi
 * tables update.
 * 
 * @author Guillaume Mary
 */
public class DeleteCommandBuilder<T extends Table<T>> implements SQLBuilder, PreparableSQLBuilder {
	
	private final Delete<T> delete;
	private final Dialect dialect;
	private final MultiTableAwareDMLNameProvider dmlNameProvider;
	
	public DeleteCommandBuilder(Delete<T> delete, Dialect dialect) {
		this.delete = delete;
		this.dialect = dialect;
		this.dmlNameProvider = new MultiTableAwareDMLNameProvider();
	}
	
	@Override
	public String toSQL() {
		StringAppender result = new StringAppender();
		appendTo(new StringSQLAppender(result, dmlNameProvider), dmlNameProvider);
		return result.toString();
	}
	
	@Override
	public ExpandableSQLAppender toPreparableSQL() {
		// We ask for SQL generation through a ExpandableSQLAppender because we need SQL placeholders for where + update clause
		ExpandableSQLAppender preparedSQLAppender = new ExpandableSQLAppender(dialect.getColumnBinderRegistry(), dmlNameProvider);
		appendTo(preparedSQLAppender, dmlNameProvider);
		
		return preparedSQLAppender;
	}
	
	private void appendTo(SQLAppender target, MultiTableAwareDMLNameProvider dmlNameProvider) {
		target.cat("delete from ");
		
		// looking for additional Tables : more than the updated one, can be found in conditions
		Set<Column<Table, Object>> whereColumns = new LinkedHashSet<>();
		delete.getCriteria().forEach(c -> {
			if (c instanceof ColumnCriterion && ((ColumnCriterion) c).getColumn() instanceof Column) {
				whereColumns.add((Column<Table, Object>) ((ColumnCriterion) c).getColumn());
				Object condition = ((ColumnCriterion) c).getCondition();
				if (condition instanceof UnitaryOperator
						&& ((UnitaryOperator) condition).getValue() instanceof ValuedVariable
						&& ((ValuedVariable) ((UnitaryOperator) condition).getValue()).getValue() instanceof Column) {
					whereColumns.add((Column) ((ValuedVariable) ((UnitaryOperator) condition).getValue()).getValue());
				}
			}
		});
		Set<Table> additionalTables = Iterables.minus(
				Iterables.collect(whereColumns, Column::getTable, HashSet::new),
				Arrays.asList(this.delete.getTargetTable()));
		
		// update of the single-table-marker
		dmlNameProvider.setMultiTable(!additionalTables.isEmpty());
		
		target.cat(this.delete.getTargetTable().getAbsoluteName())    // main table is always referenced with name (not alias)
				.catIf(dmlNameProvider.isMultiTable(), ", ");
		// additional tables (with optional alias)
		Iterator<Table> iterator = additionalTables.iterator();
		while (iterator.hasNext()) {
			Table next = iterator.next();
			target.cat(next.getAbsoluteName()).catIf(iterator.hasNext(), ", ");
		}
		
		
		// append where clause
		if (delete.getCriteria().iterator().hasNext()) {
			target.cat(" where ");
			WhereSQLBuilder whereSqlBuilder = dialect.getQuerySQLBuilderFactory().getWhereBuilderFactory().whereBuilder(this.delete.getCriteria(), dmlNameProvider);
			whereSqlBuilder.appendTo(target);
		}
	}
}

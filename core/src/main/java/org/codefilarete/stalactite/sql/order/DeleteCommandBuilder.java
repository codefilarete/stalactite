package org.codefilarete.stalactite.sql.order;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.*;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.UnitaryOperator;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
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
public class DeleteCommandBuilder implements SQLBuilder, PreparedSQLBuilder {
	
	private final Delete delete;
	private final Dialect dialect;
	private final MultiTableAwareDMLNameProvider dmlNameProvider;
	
	public DeleteCommandBuilder(Delete delete, Dialect dialect) {
		this.delete = delete;
		this.dialect = dialect;
		this.dmlNameProvider = new MultiTableAwareDMLNameProvider();
	}
	
	@Override
	public String toSQL() {
		return toSQL(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), dmlNameProvider);
	}
	
	@Override
	public PreparedSQL toPreparedSQL() {
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), dialect.getColumnBinderRegistry(), dmlNameProvider);
		String sql = toSQL(preparedSQLWrapper, dmlNameProvider);
		
		// final assembly
		PreparedSQL result = new PreparedSQL(sql, preparedSQLWrapper.getParameterBinders());
		result.setValues(preparedSQLWrapper.getValues());
		return result;
	}
	
	private String toSQL(SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		result.cat("delete from ");
		
		// looking for additional Tables : more than the updated one, can be found in conditions
		Set<Column<Table, Object>> whereColumns = new LinkedHashSet<>();
		delete.getCriteria().forEach(c -> {
			if (c instanceof ColumnCriterion) {
				whereColumns.add(((ColumnCriterion) c).getColumn());
				Object condition = ((ColumnCriterion) c).getCondition();
				if (condition instanceof UnitaryOperator && ((UnitaryOperator) condition).getValue() instanceof Column) {
					whereColumns.add((Column) ((UnitaryOperator) condition).getValue());
				}
			}
		});
		Set<Table> additionalTables = Iterables.minus(
				Iterables.collect(whereColumns, Column::getTable, HashSet::new),
				Arrays.asList(this.delete.getTargetTable()));
		
		// update of the single-table-marker
		dmlNameProvider.setMultiTable(!additionalTables.isEmpty());
		
		result.cat(this.delete.getTargetTable().getAbsoluteName())    // main table is always referenced with name (not alias)
				.catIf(dmlNameProvider.isMultiTable(), ", ");
		// additional tables (with optional alias)
		Iterator<Table> iterator = additionalTables.iterator();
		while (iterator.hasNext()) {
			Table next = iterator.next();
			result.cat(next.getAbsoluteName()).catIf(iterator.hasNext(), ", ");
		}
		
		
		// append where clause
		if (delete.getCriteria().iterator().hasNext()) {
			result.cat(" where ");
			WhereSQLBuilder whereSqlBuilder = dialect.getQuerySQLBuilderFactory().getWhereBuilderFactory().whereBuilder(this.delete.getCriteria(), dmlNameProvider);
			whereSqlBuilder.appendSQL(result);
		}
		return result.getSQL();
	}
}

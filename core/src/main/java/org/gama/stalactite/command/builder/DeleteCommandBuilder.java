package org.gama.stalactite.command.builder;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.gama.lang.StringAppender;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.command.model.Delete;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.OperandBuilder.PreparedSQLWrapper;
import org.gama.stalactite.query.builder.OperandBuilder.SQLAppender;
import org.gama.stalactite.query.builder.OperandBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.builder.SQLBuilder;
import org.gama.stalactite.query.builder.WhereBuilder;
import org.gama.stalactite.query.model.ColumnCriterion;
import org.gama.stalactite.query.model.Operand;

/**
 * A SQL builder for {@link Delete} objects
 * 
 * @author Guillaume Mary
 */
public class DeleteCommandBuilder implements SQLBuilder {
	
	private final Delete delete;
	private final MultiTableAwareDMLNameProvider dmlNameProvider;
	
	public DeleteCommandBuilder(Delete delete) {
		this.delete = delete;
		this.dmlNameProvider = new MultiTableAwareDMLNameProvider();
	}
	
	@Override
	public String toSQL() {
		return toSQL(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), dmlNameProvider);
	}
	
	private String toSQL(SQLAppender result, MultiTableAwareDMLNameProvider dmlNameProvider) {
		result.cat("delete from ");
		
		// looking for additionnal Tables : more than the updated one, can be found in conditions
		Set<Column> whereColumns = new LinkedHashSet<>();
		delete.getCriteria().forEach(c -> {
			if (c instanceof ColumnCriterion) {
				whereColumns.add(((ColumnCriterion) c).getColumn());
				Object condition = ((ColumnCriterion) c).getCondition();
				if (condition instanceof Operand && ((Operand) condition).getValue() instanceof Column) {
					whereColumns.add((Column) ((Operand) condition).getValue());
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
			WhereBuilder whereBuilder = new WhereBuilder(this.delete.getCriteria(), dmlNameProvider);
			whereBuilder.appendSQL(result);
		}
		return result.getSQL();
	}
	
	public PreparedSQL toStatement(ColumnBinderRegistry columnBinderRegistry) {
		// We ask for SQL generation through a PreparedSQLWrapper because we need SQL placeholders for where + update clause
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(new StringAppender(), dmlNameProvider), columnBinderRegistry, dmlNameProvider);
		String sql = toSQL(preparedSQLWrapper, dmlNameProvider);
		
		// final assembly
		PreparedSQL result = new PreparedSQL(sql, preparedSQLWrapper.getParameterBinders());
		result.setValues(preparedSQLWrapper.getValues());
		return result;
	}
}

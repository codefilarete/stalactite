package org.codefilarete.stalactite.spring.repository.query;

import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityCriteriaSupport;
import org.codefilarete.stalactite.query.model.LogicalOperator;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.trace.MutableBoolean;
import org.codefilarete.tool.trace.MutableLong;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.query.parser.PartTree.OrPart;

/**
 * Creates a repository query dedicated to the "exists" case.
 *
 * @param <C> domain entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteExistsProjection<C> implements RepositoryQuery {
	
	private final QueryMethod method;
	private final Accumulator<Function<Selectable<Object>, Object>, MutableBoolean, Boolean> accumulator;
	private final DerivedQuery query;
	private final Consumer<Select> selectConsumer;
	
	/**
	 * @param method the method found by Spring
	 * @param entityPersister the Stalactite domain persister
	 * @param tree result of method parsing
	 */
	public PartTreeStalactiteExistsProjection(QueryMethod method,
											  AdvancedEntityPersister<C, ?> entityPersister,
											  PartTree tree) {
		this.method = method;
		// Since exists can be done directly in SQL we'll retrieve only the primary key columns and limit the retrieved data
		// It is the way JPA does it too https://vladmihalcea.com/spring-data-exists-query/
		Set<Column<Table, ?>> pkColumns = entityPersister.<Table>getMapping().getTargetTable().getPrimaryKey().getColumns();
		selectConsumer = select -> {
			select.clear();
			pkColumns.forEach(column -> {
				select.add(column, column.getAlias());
			});
		};
		accumulator = new Accumulator<Function<Selectable<Object>, Object>, MutableBoolean, Boolean>() {
			@Override
			public Supplier<MutableBoolean> supplier() {
				return () -> new MutableBoolean(false);
			}
			
			@Override
			public BiConsumer<MutableBoolean, Function<Selectable<Object>, Object>> aggregator() {
				return (mutableBoolean, selectableObjectFunction) -> {
					mutableBoolean.setTrue();
				};
			}
			
			@Override
			public Function<MutableBoolean, Boolean> finisher() {
				return MutableBoolean::getValue;
			}
		};
		this.query = new DerivedQuery(entityPersister, tree);
		// since exists can be done directly in SQL we reduce the amount of retrieved data with a limit clause
		this.query.executableProjectionQuery.getQueryPageSupport().limit(1);
	}
	
	@Override
	public Boolean execute(Object[] parameters) {
		query.criteriaChain.consume(parameters);
		return query.executableProjectionQuery.wrapIntoExecutable().execute(accumulator);
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	class DerivedQuery extends AbstractDerivedQuery<C> {
		
		protected final ProjectionQueryCriteriaSupport<C, ?> executableProjectionQuery;
		
		private EntityCriteriaSupport<C> currentSupport;
		
		DerivedQuery(AdvancedEntityPersister<C, ?> entityPersister, PartTree tree) {
			this.executableProjectionQuery = entityPersister.newProjectionCriteriaSupport(selectConsumer);
			tree.forEach(this::append);
		}
		
		private void append(OrPart part) {
			this.currentSupport = this.executableProjectionQuery.getEntityCriteriaSupport();
			boolean nested = false;
			if (part.stream().count() > 1) {    // "if" made to avoid extra parenthesis (can be considered superfluous)
				nested = true;
				this.currentSupport = currentSupport.beginNested();
			}
			Iterator<Part> iterator = part.iterator();
			if (iterator.hasNext()) {
				append(iterator.next(), LogicalOperator.OR);
			}
			iterator.forEachRemaining(p -> this.append(p, LogicalOperator.AND));
			if (nested) {    // "if" made to avoid extra parenthesis (can be considered superfluous)
				this.currentSupport = currentSupport.endNested();
			}
		}
		
		private void append(Part part, LogicalOperator orOrAnd) {
			AccessorChain<C, Object> getter = convertToAccessorChain(part.getProperty());
			Criterion criterion = convertToCriterion(part.getType(), part.shouldIgnoreCase() != IgnoreCaseType.NEVER);
			
			this.currentSupport.add(orOrAnd, getter.getAccessors(), criterion.operator);
			super.criteriaChain.criteria.add(criterion);
		}
	}
}

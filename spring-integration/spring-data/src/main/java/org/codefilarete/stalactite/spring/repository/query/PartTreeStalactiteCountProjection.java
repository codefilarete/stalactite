package org.codefilarete.stalactite.spring.repository.query;

import java.util.Iterator;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
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
import org.codefilarete.tool.trace.MutableLong;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.query.parser.PartTree.OrPart;

/**
 * Creates a repository query dedicated to the "count" case.
 *
 * @param <C> domain entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteCountProjection<C> implements RepositoryQuery {
	
	private final Count count;
	private final QueryMethod method;
	private final Accumulator<Function<Selectable<Long>, Long>, MutableLong, Long> accumulator;
	private final DerivedQuery query;
	private final Consumer<Select> selectConsumer;
	
	/**
	 * @param method the method found by Spring
	 * @param entityPersister the Stalactite domain persister
	 * @param tree result of method parsing
	 */
	public PartTreeStalactiteCountProjection(QueryMethod method,
											 AdvancedEntityPersister<C, ?> entityPersister,
											 PartTree tree) {
		this.method = method;
		Set<Column<Table, ?>> columns = entityPersister.getMapping().getIdMapping().<Table>getIdentifierAssembler().getColumns();
		count = Operators.count(columns);
		if (tree.isDistinct()) {
			count.distinct();
		}
		selectConsumer = select -> {
			select.clear();
			select.add(count, "row_count");
		};
		accumulator = new Accumulator<Function<Selectable<Long>, Long>, MutableLong, Long>() {
			@Override
			public Supplier<MutableLong> supplier() {
				return MutableLong::new;
			}
			
			@Override
			public BiConsumer<MutableLong, Function<Selectable<Long>, Long>> aggregator() {
				return (modifiableLong, selectableObjectFunction) -> {
					Long countValue = selectableObjectFunction.apply(count);
					modifiableLong.reset(countValue);
				};
			}
			
			@Override
			public Function<MutableLong, Long> finisher() {
				return MutableLong::getValue;
			}
		};
		this.query = new DerivedQuery(entityPersister, tree);
	}
	
	@Override
	public Long execute(Object[] parameters) {
		query.condition.consume(parameters);
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
			super(entityPersister.getClassToPersist());
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
			Criterion criterion = append(part);
			
			this.currentSupport.add(orOrAnd, getter.getAccessors(), criterion.condition);
		}
	}
}

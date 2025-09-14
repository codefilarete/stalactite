package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport.EntityQueryPageSupport;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.spring.repository.query.ToCriteriaPartTreeTransformer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.trace.MutableLong;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Creates a repository query dedicated to the "count" case.
 *
 * @param <C> domain entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteCountProjection<C> implements RepositoryQuery {
	
	private final Count count;
	private final QueryMethod method;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final PartTree partTree;
	private final Accumulator<Function<Selectable<Long>, Long>, MutableLong, Long> accumulator;
	
	/**
	 * @param method the method found by Spring
	 * @param entityPersister the Stalactite domain persister
	 * @param partTree result of method parsing
	 */
	public PartTreeStalactiteCountProjection(QueryMethod method,
											 AdvancedEntityPersister<C, ?> entityPersister,
											 PartTree partTree) {
		this.method = method;
		this.entityPersister = entityPersister;
		this.partTree = partTree;
		Set<Column<Table, ?>> columns = entityPersister.getMapping().getIdMapping().<Table>getIdentifierAssembler().getColumns();
		count = Operators.count(columns);
		if (partTree.isDistinct()) {
			count.distinct();
		}
		accumulator = new Accumulator<Function<Selectable<Long>, Long>, MutableLong, Long>() {
			@Override
			public Supplier<MutableLong> supplier() {
				return MutableLong::new;
			}
			
			@Override
			public BiConsumer<MutableLong, Function<Selectable<Long>, Long>> aggregator() {
				return (modifiableLong, rowDataProvider) -> {
					modifiableLong.reset(rowDataProvider.apply(count));
				};
			}
			
			@Override
			public Function<MutableLong, Long> finisher() {
				return MutableLong::getValue;
			}
		};
	}
	
	@Override
	public Long execute(Object[] parameters) {
		ProjectionQueryCriteriaSupport<C, ?> executableEntityQuery = entityPersister.newProjectionCriteriaSupport(select -> {
			select.clear();
			select.add(count, "row_count");
		});
		// because order-by and limit clauses are compatible with count operator, we pass a page support which is not the one the executable query,
		// like a local black hole.
		EntityQueryPageSupport<C> blackHole = new EntityQueryPageSupport<>();
		ToCriteriaPartTreeTransformer<C> criteriaAppender = new ToCriteriaPartTreeTransformer<>(
				partTree,
				entityPersister.getClassToPersist(),
				executableEntityQuery.getEntityCriteriaSupport(),
				blackHole,
				blackHole);
		criteriaAppender.consume(parameters);
		return executableEntityQuery.wrapIntoExecutable().execute(accumulator);
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
}

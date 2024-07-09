package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.ModifiableInt;
import org.codefilarete.tool.trace.ModifiableLong;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.lang.Nullable;

/**
 * {@link RepositoryQuery} for Stalactite count order.
 *
 * @param <C> entity type
 * @author Guillaume Mary
 */
class PartTreeStalactiteCount<C> implements RepositoryQuery {
	
	private final QueryMethod method;
	private final Query<C> query;
	private final Count count;
	
	public PartTreeStalactiteCount(QueryMethod method, EntityPersister<C, ?> entityPersister, PartTree tree) {
		this.method = method;
		Parameters<?, ?> parameters = method.getParameters();
		
		boolean recreationRequired = parameters.hasDynamicProjection() || parameters.potentiallySortsDynamically();
		
		try {
			Set<Column> columns = ((ConfiguredPersister) entityPersister).getMapping().getIdMapping().getIdentifierAssembler().getColumns();
			count = Operators.count(Iterables.first(columns));
			this.query = new Query<>(entityPersister, tree);
			
		} catch (RuntimeException o_O) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s! %s", method, o_O.getMessage()), o_O);
		}
	}
	
	@Override
	@Nullable
	public Long execute(Object[] parameters) {
		query.criteriaChain.consume(parameters);
		return query.executableEntityQuery.execute(new Accumulator<Function<Selectable<Long>, Long>, ModifiableLong, Long>() {
			@Override
			public Supplier<ModifiableLong> supplier() {
				return ModifiableLong::new;
			}
			
			@Override
			public BiConsumer<ModifiableLong, Function<Selectable<Long>, Long>> aggregator() {
				return (modifiableLong, selectableObjectFunction) -> {
					Long countValue = selectableObjectFunction.apply(count);
					modifiableLong.reset(countValue);
				};
			}
			
			@Override
			public Function<ModifiableLong, Long> finisher() {
				return ModifiableLong::getValue;
			}
		});
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	class Query<T> extends AbstractQuery<T> {
		
		private static final String COUNT_ALIAS = "row_count";
		
		protected final ExecutableProjectionQuery<T> executableEntityQuery;
		
		Query(EntityPersister<T, ?> entityPersister, PartTree tree) {
			super(entityPersister);
			Holder<ExecutableProjectionQuery<T>> resultHolder = new Holder<>();
			ModifiableInt partIndex = new ModifiableInt();
			tree.forEach(orPart -> {
				orPart.forEach(part -> {
							append(part, resultHolder, partIndex.getValue());
							partIndex.increment();
						}
				);
			});
			executableEntityQuery = resultHolder.get();
		}
		
		private void append(Part part, Holder<ExecutableProjectionQuery<T>> resultHolder, int partIndex) {
			Criterion criterion = convertToOperator(part.getType());
			// entityPersister doesn't support the creation of a ExecutableProjectionQuery from scratch and requires to
			// create it from entityPersister.selectWhere : we call it for first part
			if (partIndex == 0) {
				ExecutableProjectionQuery<T> criteriaHook = entityPersister.selectProjectionWhere(select ->  {
					select.clear();
					select.add(count, COUNT_ALIAS);
				}, convertToAccessorChain(part.getProperty()), criterion.operator);
				resultHolder.set(criteriaHook);
			} else {
				resultHolder.get().and(convertToAccessorChain(part.getProperty()), criterion.operator);
			}
			this.criteriaChain.criteria.add(criterion);
		}
		
	}
}

package org.codefilarete.stalactite.sql.spring.repository.query;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.EntityPersister.ExecutableProjectionQuery;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.operator.Count;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.trace.ModifiableBoolean;
import org.codefilarete.tool.trace.ModifiableLong;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link RepositoryQuery} for Stalactite count order.
 *
 * @param <C> entity type
 * @author Guillaume Mary
 */
class PartTreeStalactiteProjection<C, R> implements RepositoryQuery {
	
	/**
	 * Creates a projection dedicated to "count".
	 * It can't be done by inheriting from {@link PartTreeStalactiteProjection} because the constructor would have to do too many
	 * things which are no compatible with constructor limitations before calling super(..)
	 * 
	 * @param method the method found by Spring
	 * @param entityPersister the Stalactite domain persister
	 * @param tree result of method parsing
	 * @return a {@link PartTreeStalactiteProjection} that implements an sql count
	 * @param <T> domain entity type
	 */
	static <T> PartTreeStalactiteProjection<T, Long> forCount(QueryMethod method, EntityPersister<T, ?> entityPersister, PartTree tree) {
		Set<Column<?, ?>> columns = ((ConfiguredPersister) entityPersister).getMapping().getIdMapping().getIdentifierAssembler().getColumns();
		Count count = Operators.count(columns);
		if (tree.isDistinct()) {
			count.distinct();
		}
		return new PartTreeStalactiteProjection<>(method, entityPersister, tree, select -> {
			select.clear();
			select.add(count, "row_count");
		}, new Accumulator<Function<Selectable<Long>, Long>, ModifiableLong, Long>() {
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
	
	/**
	 * Creates a projection dedicated to "exists".
	 * 
	 * @param method the method found by Spring
	 * @param entityPersister the Stalactite domain persister
	 * @param tree result of method parsing
	 * @return a {@link PartTreeStalactiteProjection} that implements an sql count
	 * @param <T> domain entity type
	 */
	static <T> PartTreeStalactiteProjection<T, Boolean> forExists(QueryMethod method, EntityPersister<T, ?> entityPersister, PartTree tree) {
		// Since exists can be done directly in SQL we'll retrieve only primary key columns and limit retrieved data
		// It is the way JPA does it too https://vladmihalcea.com/spring-data-exists-query/
		Set<Column<?, ?>> pkColumns = ((ConfiguredPersister) entityPersister).getMapping().getTargetTable().getPrimaryKey().getColumns();
		PartTreeStalactiteProjection<T, Boolean> partTreeExists = new PartTreeStalactiteProjection<>(method, entityPersister, tree, selectables -> {
			selectables.clear();
			pkColumns.forEach(column -> {
				selectables.add(column, column.getAlias());
			});
		}, new Accumulator<Function<Selectable<Object>, Object>, ModifiableBoolean, Boolean>() {
			@Override
			public Supplier<ModifiableBoolean> supplier() {
				return () -> new ModifiableBoolean(false);
			}
			
			@Override
			public BiConsumer<ModifiableBoolean, Function<Selectable<Object>, Object>> aggregator() {
				return (modifiableBoolean, selectableObjectFunction) -> {modifiableBoolean.setTrue();
				};
			}
			
			@Override
			public Function<ModifiableBoolean, Boolean> finisher() {
				return ModifiableBoolean::getValue;
			}
		});
		// since exists can be done directly in SQL we reduce the amount of retrieved data with limit
		partTreeExists.getQuery().executableProjectionQuery.limit(1);
		return partTreeExists;
	}
	
	private final QueryMethod method;
	private final Accumulator<Function<Selectable<Object>, Object>, ?, R> accumulator;
	private final DerivedQuery<C> query;
	private final Consumer<Select> selectConsumer;
	
	public <O> PartTreeStalactiteProjection(
			QueryMethod method,
			EntityPersister<C, ?> entityPersister,
			PartTree tree,
			Consumer<Select> selectConsumer,
			Accumulator<Function<Selectable<O>, O>, ?, R> accumulator) {
		this.method = method;
		this.accumulator = (Accumulator)  accumulator;
		Parameters<?, ?> parameters = method.getParameters();
		
		boolean recreationRequired = parameters.hasDynamicProjection() || parameters.potentiallySortsDynamically();
		
		// TODO : Handle select query optimization by selecting what's only necessary to the result. Have a look at JpaQueryCreator#complete(..)
		// TODO : It can't be done for now in Stalactite because it lacks a tree of properties : EntityJoinTree doesn't have the info and is quite hard to add.
		
		try {
			this.selectConsumer = selectConsumer;
			this.query = new DerivedQuery<>(entityPersister, tree);
			
		} catch (RuntimeException o_O) {
			throw new IllegalArgumentException(
					String.format("Failed to create query for method %s! %s", method, o_O.getMessage()), o_O);
		}
	}
	
	public DerivedQuery<C> getQuery() {
		return query;
	}
	
	@Override
	public R execute(Object[] parameters) {
		query.criteriaChain.consume(parameters);
		R result = query.executableProjectionQuery.execute(accumulator);
		
		// - isProjecting() is for case of return type is not domain one (nor a compound one by Collection or other)
		// - hasDynamicProjection() is for case of method that gives the expected returned type as a last argument (or a compound one by Collection or other)
		if (method.getResultProcessor().getReturnedType().isProjecting() || method.getParameters().hasDynamicProjection()) {
			ParameterAccessor accessor = new ParametersParameterAccessor(method.getParameters(), parameters);
			// withDynamicProjection() handles the 2 cases of the "if" (with some not obvious algorithm)
			return method.getResultProcessor().withDynamicProjection(accessor).processResult(result);
		} else {
			return result;
		}
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
	
	class DerivedQuery<T> extends AbstractDerivedQuery<T> {
		
		protected final ExecutableProjectionQuery<T, ?> executableProjectionQuery;
		
		DerivedQuery(EntityPersister<T, ?> entityPersister, PartTree tree) {
			this.executableProjectionQuery = entityPersister.selectProjectionWhere(selectConsumer);
			tree.forEach(orPart -> orPart.forEach(this::append));
		}
		
		private void append(Part part) {
			Criterion criterion = convertToCriterion(part.getType(), part.shouldIgnoreCase() != IgnoreCaseType.NEVER);
			this.executableProjectionQuery.and(convertToAccessorChain(part.getProperty()), criterion.operator);
			this.criteriaChain.criteria.add(criterion);
		}
	}
}

package org.codefilarete.stalactite.spring.repository.query.projection;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.engine.ExecutableProjection.ProjectionDataProvider;
import org.codefilarete.stalactite.engine.runtime.AdvancedEntityPersister;
import org.codefilarete.stalactite.engine.runtime.ProjectionQueryCriteriaSupport;
import org.codefilarete.stalactite.spring.repository.query.derivation.ToCriteriaPartTreeTransformer;
import org.codefilarete.stalactite.spring.repository.query.derivation.ToCriteriaPartTreeTransformer.Condition;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.trace.MutableBoolean;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Creates a repository query dedicated to the "exists" case.
 *
 * @param <C> domain entity type
 * @author Guillaume Mary
 */
public class PartTreeStalactiteExistsProjection<C> implements RepositoryQuery {
	
	private final QueryMethod method;
	private final AdvancedEntityPersister<C, ?> entityPersister;
	private final Accumulator<ProjectionDataProvider, MutableBoolean, Boolean> accumulator;
	private final ToCriteriaPartTreeTransformer<C> criteriaAppender;
	
	/**
	 * @param method the method found by Spring
	 * @param entityPersister the Stalactite domain persister
	 * @param partTree result of method parsing
	 */
	public PartTreeStalactiteExistsProjection(QueryMethod method,
											  AdvancedEntityPersister<C, ?> entityPersister,
											  PartTree partTree) {
		this.method = method;
		this.entityPersister = entityPersister;
		accumulator = new Accumulator<ProjectionDataProvider, MutableBoolean, Boolean>() {
			@Override
			public Supplier<MutableBoolean> supplier() {
				return () -> new MutableBoolean(false);
			}
			
			@Override
			public BiConsumer<MutableBoolean, ProjectionDataProvider> aggregator() {
				return (mutableBoolean, selectableObjectFunction) -> {
					mutableBoolean.setTrue();
				};
			}
			
			@Override
			public Function<MutableBoolean, Boolean> finisher() {
				return MutableBoolean::getValue;
			}
		};
		criteriaAppender = new ToCriteriaPartTreeTransformer<>(
				partTree,
				entityPersister.getClassToPersist());
	}
	
	@Override
	public Boolean execute(Object[] parameters) {
		// Since exists can be done directly in SQL we'll retrieve only the primary key columns and limit the retrieved data
		// It is the way JPA does it too https://vladmihalcea.com/spring-data-exists-query/
		Set<Column<Table, ?>> pkColumns = entityPersister.<Table>getMapping().getTargetTable().getPrimaryKey().getColumns();
		ProjectionQueryCriteriaSupport<C, ?> executableEntityQuery = entityPersister.newProjectionCriteriaSupport(select -> {
			select.clear();
			pkColumns.forEach(column -> {
				select.add(column, column.getAlias());
			});
		});
		// since exists can be done directly in SQL we reduce the amount of retrieved data with a limit clause
		executableEntityQuery.getQueryPageSupport().limit(1);
		Condition condition = criteriaAppender.applyTo(
				executableEntityQuery.getEntityCriteriaSupport(),
				executableEntityQuery.getQueryPageSupport(),
				executableEntityQuery.getQueryPageSupport());
		condition.consume(parameters);
		return executableEntityQuery.wrapIntoExecutable().execute(accumulator);
	}
	
	@Override
	public QueryMethod getQueryMethod() {
		return method;
	}
}

package org.gama.stalactite.persistence.engine.cascade;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Nullable;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.cascade.EntityMappingStrategyTreeJoinPoint.JoinType;
import org.gama.stalactite.persistence.engine.cascade.EntityMappingStrategyTreeRowTransformer.EntityInflater;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.EntitySelectExecutor;
import org.gama.stalactite.persistence.query.IEntitySelectExecutor;
import org.gama.stalactite.persistence.query.RelationalEntityCriteria;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.sql.ConnectionProvider;

import static java.util.Collections.emptyList;

/**
 * Persister for entity with multiple joined tables with "foreign key = primary key".
 * A main table is defined by the {@link ClassMappingStrategy} passed to constructor. Complementary tables are defined
 * with {@link EntityMappingStrategyTreeSelectBuilder#addRelationJoin(String, IEntityMappingStrategy, Column, Column, JoinType, BeanRelationFixer)}.
 * Entity load is defined by a select that joins all tables, each {@link ClassMappingStrategy} is called to complete
 * entity loading.
 * 
 * In the orm module this class replace {@link Persister} in case of single table, because it has methods for join support whereas {@link Persister}
 * doesn't.
 * 
 * @param <C> the main class to be persisted
 * @param <I> the type of main class identifiers
 * @param <T> the main target table
 * @author Guillaume Mary
 */
public class JoinedTablesPersister<C, I, T extends Table> extends Persister<C, I, T> implements IEntityConfiguredJoinedTablesPersister<C, I> {
	
	/** Support for {@link EntityCriteria} query execution */
	private IEntitySelectExecutor<C> entitySelectExecutor;
	/** Support for defining Entity criteria on {@link #newWhere()} */
	private final EntityCriteriaSupport<C> criteriaSupport;
	
	public JoinedTablesPersister(PersistenceContext persistenceContext, IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
		this(mainMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionConfiguration());
	}
	
	public JoinedTablesPersister(IEntityMappingStrategy<C, I, T> mainMappingStrategy, Dialect dialect, IConnectionConfiguration connectionConfiguration) {
		super(mainMappingStrategy, dialect, connectionConfiguration);
		this.criteriaSupport = new EntityCriteriaSupport<>(getMappingStrategy());
		this.entitySelectExecutor = newEntitySelectExecutor(dialect);
	}
	
	@Override
	protected ISelectExecutor<C, I> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
													  Dialect dialect) {
		return new EntityMappingStrategyTreeSelectExecutor<>(mappingStrategy, dialect, connectionProvider);
	}
	
	protected IEntitySelectExecutor<C> newEntitySelectExecutor(Dialect dialect) {
		return new EntitySelectExecutor<>(getEntityMappingStrategyTreeSelectExecutor().getEntityMappingStrategyTreeSelectBuilder(), getConnectionProvider(), dialect.getColumnBinderRegistry());
	}
	
	/**
	 * Gives access to the select executor for further manipulations on {@link EntityMappingStrategyTreeSelectBuilder} for advanced usage
	 * @return parent {@link org.gama.stalactite.persistence.engine.ISelectExecutor} casted as a {@link EntityMappingStrategyTreeSelectExecutor}
	 */
	public EntityMappingStrategyTreeSelectExecutor<C, I, T> getEntityMappingStrategyTreeSelectExecutor() {
		return (EntityMappingStrategyTreeSelectExecutor<C, I, T>) super.getSelectExecutor();
	}
	
	@Override
	public EntityMappingStrategyTreeSelectBuilder<C, I, ?> getEntityMappingStrategyTreeSelectBuilder() {
		return getEntityMappingStrategyTreeSelectExecutor().getEntityMappingStrategyTreeSelectBuilder();
	}
	
	/**
	 * Overriden to implement a load by joining tables
	 * 
	 * @param ids entity identifiers
	 * @return a List of loaded entities corresponding to identifiers passed as parameter
	 */
	@Override
	protected List<C> doSelect(Iterable<I> ids) {
		return getSelectExecutor().select(ids);
	}
	
	/**
	 * Gives the {@link EntityInflater} of a join node.
	 * Node name must be known so one should have kept it from the
	 * {@link EntityMappingStrategyTreeSelectBuilder#addRelationJoin(String, IEntityMappingStrategy, Column, Column, JoinType, BeanRelationFixer)}
	 * return, else, since node naming strategy is not exposed it is not recommanded to use this method out of any test or debug purpose. 
	 * 
	 * @param nodeName a name of a added strategy
	 * @return the {@link ClassMappingStrategy} behind a join node, null if not found
	 */
	public EntityInflater giveJoinedStrategy(String nodeName) {
		return Nullable.nullable(getEntityMappingStrategyTreeSelectExecutor().getEntityMappingStrategyTreeSelectBuilder().getTree(nodeName))
				.map(EntityMappingStrategyTree::getStrategy).get();
	}
	
	@Override
	public Set<Table> giveImpliedTables() {
		return getEntityMappingStrategyTreeSelectExecutor().getEntityMappingStrategyTreeSelectBuilder().giveTables();
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invokations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * <strong>As for now aggregate result is truncated to entities returned by SQL selection : for example, if criteria on collection is used,
	 * only entities returned by SQL criteria will be loaded. This does not respect aggregate principle and should be enhanced in future.</strong>
	 * 
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(getter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	/**
	 * Creates a query which criteria target mapped properties
	 * <strong>As for now aggregate result is truncated to entities returned by SQL selection : for example, if criteria on collection is used,
	 * only entities returned by SQL criteria will be loaded. This does not respect aggregate principle and should be enhanced in future.</strong>
	 *
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	@Override
	public <O> RelationalExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	private RelationalExecutableEntityQuery<C> wrapIntoExecutable(EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableFunction<ExecutableQuery, List<C>>) ExecutableQuery::execute,
						() -> getPersisterListener().doWithSelectListener(emptyList(), () -> entitySelectExecutor.loadGraph(localCriteriaSupport.getCriteria())))
				.redirect(CriteriaProvider::getCriteria, localCriteriaSupport::getCriteria)
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				.build((Class<RelationalExecutableEntityQuery<C>>) (Class) RelationalExecutableEntityQuery.class);
	}
	
	/**
	 * Select all instances with all relations fetched.
	 * 
	 * @return all instance found in database
	 */
	public List<C> selectAll() {
		return getPersisterListener().doWithSelectListener(emptyList(), () ->
				entitySelectExecutor.loadGraph(newWhere().getCriteria())
		);
	}
	
	public EntityCriteriaSupport<C> getCriteriaSupport() {
		return criteriaSupport;
	}
	
	/**
	 * Implementation for simple one-to-one cases : we add our joins to given persister
	 */
	@Override
	public <SRC, T1 extends Table, T2 extends Table> void joinAsOne(IJoinedTablesPersister<SRC, I> sourcePersister,
								Column<T1, I> leftColumn, Column<T2, I> rightColumn, BeanRelationFixer<SRC, C> beanRelationFixer, boolean optional) {
		
		// We use our own select system since SelectListener is not aimed at joining table
		String createdJoinNodeName = sourcePersister.getEntityMappingStrategyTreeSelectBuilder().addRelationJoin(
				EntityMappingStrategyTreeSelectBuilder.ROOT_STRATEGY_NAME,
				(IEntityMappingStrategy) this.getMappingStrategy(),
				leftColumn,
				rightColumn,
				optional ? JoinType.OUTER : JoinType.INNER,
				beanRelationFixer);
		
		copyJoinsRootTo(sourcePersister.getEntityMappingStrategyTreeSelectBuilder(), createdJoinNodeName);
	}
	
	/**
	 * Implementation for simple one-to-many cases : we add our joins to given persister
	 */
	@Override
	public <SRC, T1 extends Table, T2 extends Table, J> void joinAsMany(IJoinedTablesPersister<SRC, J> sourcePersister,
																	 Column<T1, J> leftColumn, Column<T2, J> rightColumn,
																	 BeanRelationFixer<SRC, C> beanRelationFixer, String joinName, boolean optional) {
		
		String createdJoinNodeName = sourcePersister.getEntityMappingStrategyTreeSelectBuilder().addRelationJoin(
				joinName,
				(IEntityMappingStrategy) getMappingStrategy(),
				leftColumn,
				rightColumn,
				optional ? JoinType.OUTER : JoinType.INNER,
				beanRelationFixer);
		
		// adding our subgraph select to source persister
		copyJoinsRootTo(sourcePersister.getEntityMappingStrategyTreeSelectBuilder(), createdJoinNodeName);
	}
	
	@Override
	public <E, ID, T extends Table> void copyJoinsRootTo(EntityMappingStrategyTreeSelectBuilder<E, ID, T> entityMappingStrategyTreeSelectBuilder, String joinName) {
		getEntityMappingStrategyTreeSelectBuilder().getRoot().copyTo(entityMappingStrategyTreeSelectBuilder, joinName);
	}
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 * @param <C> type of object returned by query execution
	 */
	public interface RelationalExecutableEntityQuery<C> extends ExecutableEntityQuery<C>, CriteriaProvider, RelationalEntityCriteria<C> {
		
		<O> RelationalExecutableEntityQuery<C> and(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		<O> RelationalExecutableEntityQuery<C> and(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
		
		<O> RelationalExecutableEntityQuery<C> or(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		<O> RelationalExecutableEntityQuery<C> or(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
		
		<A, B> RelationalExecutableEntityQuery<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
		
		<S extends Collection<A>, A, B> RelationalExecutableEntityQuery<C> andMany(SerializableFunction<C, S> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
		
	}
	
	/**
	 * Interface that allows access to the {@link CriteriaChain} of the {@link EntityCriteriaSupport} wrapped into the proxy returned by
	 * {@link #wrapIntoExecutable(EntityCriteriaSupport)}.
	 * Mainly created from test purpose that requires access to underlying objects
	 */
	public interface CriteriaProvider {
		
		CriteriaChain getCriteria();
		
	}
}

package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Queue;
import java.util.Set;
import javax.annotation.Nullable;

import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.stalactite.engine.runtime.DeleteExecutor;
import org.codefilarete.stalactite.engine.runtime.InsertExecutor;
import org.codefilarete.stalactite.engine.runtime.RelationIds;
import org.codefilarete.stalactite.engine.runtime.UpdateExecutor;
import org.codefilarete.stalactite.engine.runtime.WriteListenerWrapper;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Duo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <C> the main class to be persisted
 * @param <I> the type of main class identifiers
 * @param <T> the main target table
 * @author Guillaume Mary
 */
public class EntityWriter<C, I, T extends Table<T>>
		extends WriteListenerWrapper<C, I>
		implements EntityWriteExecutor<C, I> {
	
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	/**
	 * Current storage of entities to be loaded during the 2-Phases load algorithm.
	 * Tracked as a {@link Queue} to solve resource cleaning issue in case of recursive polymorphism. This may be solved by avoiding to have a static field
	 */
	// TODO : try a non-static field to remove Queue usage which impacts code complexity
	@SuppressWarnings("java:S5164" /* remove() is called by SecondPhaseRelationLoader.afterSelect() */)
	private static final ThreadLocal<Queue<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>>> CURRENT_2PHASES_LOAD_CONTEXT = new ThreadLocal<>();
	
	private final BeanPersister<C, I, T> persister;
//	private final PersistExecutor<C> persistExecutor;
	protected final Dialect dialect;
	
	public EntityWriter(DefaultEntityMapping<C, I, T> mainMappingStrategy,
	                    Dialect dialect,
	                    ConnectionConfiguration connectionConfiguration) {
		this.persister = new BeanPersister<>(mainMappingStrategy, dialect, connectionConfiguration);
		this.dialect = dialect;
		// we redirect all invocations to ourselves because targeted methods invoke their listeners
		// this.persistExecutor = PersistExecutor.forPersister(this);
	}
	
	public EntityWriter(DefaultEntityMapping<C, I, T> mainMappingStrategy,
	                    @Nullable VersioningStrategy<C, ?> versioningStrategy,
	                    Dialect dialect,
	                    ConnectionConfiguration connectionConfiguration) {
		this.persister = new BeanPersister<>(mainMappingStrategy, dialect, connectionConfiguration);
		this.dialect = dialect;
		// we redirect all invocations to ourselves because targeted methods invoke their listeners
//		this.persistExecutor = PersistExecutor.forPersister(this);
		
		if (versioningStrategy != null) {
			getUpdateExecutor().setVersioningStrategy(versioningStrategy);
			getInsertExecutor().setVersioningStrategy(versioningStrategy);
		}
	}
	
	@Override
	public I getId(C entity) {
		return this.persister.getId(entity);
	}
	
	public InsertExecutor<C, I, T> getInsertExecutor() {
		return persister.getInsertExecutor();
	}
	
	public UpdateExecutor<C, I, T> getUpdateExecutor() {
		return persister.getUpdateExecutor();
	}
	
	public DeleteExecutor<C, I, T> getDeleteExecutor() {
		return persister.getDeleteExecutor();
	}
	
	@Override
	public EntityMapping<C, I, T> getMapping() {
		return persister.getMapping();
	}
	
//	@Override
//	public boolean isNew(C entity) {
//		return persister.isNew(entity);
//	}
//	
//	@Override
//	public Class<C> getClassToPersist() {
//		return persister.getClassToPersist();
//	}
//	
//	@Override
//	protected void doPersist(Iterable<? extends C> entities) {
//		persistExecutor.persist(entities);
//	}
	
	@Override
	public void doDelete(Iterable<? extends C> entities) {
		persister.delete(entities);
	}
	
	@Override
	public void doDeleteById(Iterable<? extends C> entities) {
		persister.deleteById(entities);
	}
	
	@Override
	public void doInsert(Iterable<? extends C> entities) {
		persister.insert(entities);
	}
	
	@Override
	public void doUpdateById(Iterable<? extends C> entities) {
		persister.updateById(entities);
	}
	
	@Override
	public void doUpdate(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		persister.update(differencesIterable, allColumnsStatement);
	}
}

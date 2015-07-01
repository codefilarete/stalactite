package org.gama.stalactite.persistence.engine;

import org.gama.lang.bean.IDelegate;
import org.gama.lang.bean.IDelegateWithResult;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.id.AutoAssignedIdentifierGenerator;
import org.gama.stalactite.persistence.id.IdentifierGenerator;
import org.gama.stalactite.persistence.id.PostInsertIdentifierGenerator;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * CRUD Persistent features dedicated to an entity class. Entry point for persistent operations on entities.
 * 
 * @author Guillaume Mary
 */
public class Persister<T> {
	
	private PersistenceContext persistenceContext;
	private ClassMappingStrategy<T> mappingStrategy;
	private PersisterListener<T> persisterListener = new PersisterListener<>();
	private PersisterExecutor<T> persisterExecutor;
	
	public Persister(PersistenceContext persistenceContext, ClassMappingStrategy<T> mappingStrategy) {
		this.persistenceContext = persistenceContext;
		this.mappingStrategy = mappingStrategy;
		this.persisterExecutor = new PersisterExecutor<>(mappingStrategy, configureIdentifierFixer(mappingStrategy),
				persistenceContext.getJDBCBatchSize(), persistenceContext.getTransactionManager(),
				persistenceContext.getDialect().getColumnBinderRegistry());
	}
	
	public PersistenceContext getPersistenceContext() {
		return persistenceContext;
	}
	
	public ClassMappingStrategy<T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	protected IIdentifierFixer<T> configureIdentifierFixer(ClassMappingStrategy<T> mappingStrategy) {
		IIdentifierFixer<T> identifierFixer = null;
		// preparing identifier instance 
		if (mappingStrategy != null) {
			IdentifierGenerator identifierGenerator = mappingStrategy.getIdentifierGenerator();
			if (!(identifierGenerator instanceof AutoAssignedIdentifierGenerator)
					&& !(identifierGenerator instanceof PostInsertIdentifierGenerator)) {
				identifierFixer = new IdentifierFixer<T>(identifierGenerator, mappingStrategy);
			} else {
				identifierFixer = NoopIdentifierFixer.INSTANCE;
			}
		} // else // rare cases but necessary
		return identifierFixer;
	}
	
	public void setPersisterListener(PersisterListener<T> persisterListener) {
		// prevent from null instance
		if (persisterListener != null) {
			this.persisterListener = persisterListener;
		}
	}
	
	/**
	 * should never be null for simplicity (skip "if" on each CRUD method)
	 * 
	 * @return not null
	 */
	public PersisterListener<T> getPersisterListener() {
		return persisterListener;
	}
	
	public void persist(T t) {
		// determine insert or update operation
		if (isNew(t)) {
			insert(t);
		} else {
			updateRoughly(t);
		}
	}
	
	public void persist(Iterable<T> iterable) {
		// determine insert or update operation
		List<T> toInsert = new ArrayList<>(20);
		List<T> toUpdate = new ArrayList<>(20);
		for (T t : iterable) {
			if (isNew(t)) {
				toInsert.add(t);
			} else {
				toUpdate.add(t);
			}
		}
		if (!toInsert.isEmpty()) {
			insert(toInsert);
		}
		if (!toUpdate.isEmpty()) {
			updateRoughly(toUpdate);
		}
	}
	
	public void insert(T t) {
		insert(Collections.singletonList(t));
	}
	
	public void insert(final Iterable<T> iterable) {
		getPersisterListener().doWithInsertListener(iterable, new IDelegate() {
			@Override
			public void execute() {
				doInsert(iterable);
			}
		});
	}
	
	protected void doInsert(Iterable<T> iterable) {
		persisterExecutor.insert(iterable);
	}
	
	public void updateRoughly(T t) {
		updateRoughly(Collections.singletonList(t));
	}
	
	/**
	 * Update roughly some instances: no difference are computed, only update statements (full column) are applied.
	 * @param iterable iterable of instances
	 */
	public void updateRoughly(final Iterable<T> iterable) {
		getPersisterListener().doWithUpdateRouglyListener(iterable, new IDelegate() {
			@Override
			public void execute() {
				doUpdateRoughly(iterable);
			}
		});
	}
	
	protected void doUpdateRoughly(Iterable<T> iterable) {
		persisterExecutor.updateRoughly(iterable);
	}
	
	public void update(T modified, T unmodified, boolean allColumnsStatement) {
		update(Collections.singletonList((Entry<T, T>) new HashMap.SimpleImmutableEntry<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Update instances that has changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 * 
	 * @param differencesIterable pairs of modified-unmodified instances, used to compute differences side by side
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be..
	 *                            Pass true gives more chance for JDBC batch to be used. 
	 */
	public void update(final Iterable<Entry<T, T>> differencesIterable, final boolean allColumnsStatement) {
		getPersisterListener().doWithUpdateListener(differencesIterable, new IDelegate() {
			@Override
			public void execute() {
				doUpdate(differencesIterable, allColumnsStatement);
			}
		});
	}
	
	protected void doUpdate(Iterable<Entry<T, T>> differencesIterable, boolean allColumnsStatement) {
		persisterExecutor.update(differencesIterable, allColumnsStatement);
	}
	
	public void delete(T t) {
		delete(Collections.singletonList(t));
	}
	
	public void delete(final Iterable<T> iterable) {
		getPersisterListener().doWithDeleteListener(iterable, new IDelegate() {
			@Override
			public void execute() {
				doDelete(iterable);
			}
		});
	}
	
	protected void doDelete(Iterable<T> iterable) {
		persisterExecutor.delete(iterable);
	}
	
	/**
	 * Indicates if a bean is persisted or not, implemented on null identifier 
	 * @param t a bean
	 * @return true if bean's id is null, false otherwise
	 */
	public boolean isNew(T t) {
		return mappingStrategy.getId(t) == null;
	}
	
	public T select(final Serializable id) {
		return Iterables.first(select(Collections.singleton(id)));
	}
	
	public List<T> select(final Iterable<Serializable> ids) {
		if (!Iterables.isEmpty(ids)) {
			return getPersisterListener().doWithSelectListener(ids, new IDelegateWithResult<List<T>>() {
				@Override
				public List<T> execute() {
					return doSelect(ids);
				}
			});
		} else {
			throw new IllegalArgumentException("Non selectable entity " + mappingStrategy.getClassToPersist() + " because of null id");
		}
	}
	
	protected List<T> doSelect(Iterable<Serializable> ids) {
		return persisterExecutor.select(ids);
	}
	
	/**
	 * Internal interface to specify contract of classes that can fix an identifier to an instance.
	 * @param <T>
	 */
	protected interface IIdentifierFixer<T> {
		
		void fixId(T t);
		
	}
	
	protected static class IdentifierFixer<T> implements IIdentifierFixer<T> {
		
		private IdentifierGenerator identifierGenerator;
		
		private ClassMappingStrategy<T> mappingStrategy;
		
		private IdentifierFixer(IdentifierGenerator identifierGenerator, ClassMappingStrategy<T> mappingStrategy) {
			this.identifierGenerator = identifierGenerator;
			this.mappingStrategy = mappingStrategy;
		}
		
		public void fixId(T t) {
			mappingStrategy.setId(t, this.identifierGenerator.generate());
		}
	}
	
	protected static class NoopIdentifierFixer implements IIdentifierFixer {
		
		public static final NoopIdentifierFixer INSTANCE = new NoopIdentifierFixer();
		
		private NoopIdentifierFixer() {
			super();
		}
		
		@Override
		public final void fixId(Object o) {
		}
	}
}

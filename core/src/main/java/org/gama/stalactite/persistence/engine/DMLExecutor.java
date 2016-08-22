package org.gama.stalactite.persistence.engine;

import org.gama.sql.SimpleConnectionProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;

/**
 * CRUD Persistent features dedicated to an entity class. Kind of sliding door of {@link Persister} aimed at running
 * actions for it.
 * 
 * @author Guillaume Mary
 */
public abstract class DMLExecutor<T, I> {
	
	private final ClassMappingStrategy<T, I> mappingStrategy;
	private final org.gama.stalactite.persistence.engine.ConnectionProvider connectionProvider;
	private final DMLGenerator dmlGenerator;
	private final int inOperatorMaxSize;
	
	public DMLExecutor(ClassMappingStrategy<T, I> mappingStrategy, org.gama.stalactite.persistence.engine.ConnectionProvider connectionProvider,
					   DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		this.mappingStrategy = mappingStrategy;
		this.connectionProvider = connectionProvider;
		this.dmlGenerator = dmlGenerator;
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
	
	public ClassMappingStrategy<T, I> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public org.gama.stalactite.persistence.engine.ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	/**
	 * Implementation that gives the ConnectionProvider.getCurrentConnection() of instanciation time
	 */
	protected class ConnectionProvider extends SimpleConnectionProvider {
		
		public ConnectionProvider() {
			super(connectionProvider.getCurrentConnection());
		}
	}
}

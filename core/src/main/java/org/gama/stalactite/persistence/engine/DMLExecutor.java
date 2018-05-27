package org.gama.stalactite.persistence.engine;

import org.gama.sql.ConnectionProvider;
import org.gama.sql.SimpleConnectionProvider;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Table;

/**
 * CRUD Persistent features dedicated to an entity class. Kind of sliding door of {@link Persister} aimed at running
 * actions for it.
 * 
 * @author Guillaume Mary
 */
public abstract class DMLExecutor<C, I, T extends Table> {
	
	private final ClassMappingStrategy<C, I, T> mappingStrategy;
	private final ConnectionProvider connectionProvider;
	private final DMLGenerator dmlGenerator;
	private final int inOperatorMaxSize;
	
	public DMLExecutor(ClassMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
					   DMLGenerator dmlGenerator, int inOperatorMaxSize) {
		this.mappingStrategy = mappingStrategy;
		this.connectionProvider = connectionProvider;
		this.dmlGenerator = dmlGenerator;
		this.inOperatorMaxSize = inOperatorMaxSize;
	}
	
	public ClassMappingStrategy<C, I, T> getMappingStrategy() {
		return mappingStrategy;
	}
	
	public ConnectionProvider getConnectionProvider() {
		return connectionProvider;
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public int getInOperatorMaxSize() {
		return inOperatorMaxSize;
	}
	
	/**
	 * Implementation that gives the {@link ConnectionProvider#getCurrentConnection()} of instanciation time
	 */
	protected class CurrentConnectionProvider extends SimpleConnectionProvider {
		
		public CurrentConnectionProvider() {
			super(connectionProvider.getCurrentConnection());
		}
	}
}

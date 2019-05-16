package org.gama.stalactite.persistence.engine;

import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Table;

/**
 * A definition of a foreign key coming from JDBC metadata
 * 
 * @author Guillaume Mary
 */
class JdbcForeignKey {
	private final String name;
	private final String srcColumnName;
	private final String srcTableName;
	private final String targetColumnName;
	private final String targetTableName;
	
	
	JdbcForeignKey(String name, String srcTableName, String srcColumnName, String targetTableName, String targetColumnName) {
		this.name = name;
		this.srcColumnName = srcColumnName;
		this.srcTableName = srcTableName;
		this.targetColumnName = targetColumnName;
		this.targetTableName = targetTableName;
	}
	
	public JdbcForeignKey(ForeignKey<? extends Table, ? extends Table> fk) {
		this(fk.getName(),
				fk.getTable().getName(),
				Iterables.first(fk.getColumns()).getName(),
				fk.getTargetTable().getName(),
				Iterables.first(fk.getTargetColumns()).getName());
	}
	
	String getSignature() {
		return "JdbcForeignKey{" +
				"name='" + name + '\'' +
				", srcColumnName='" + srcColumnName + '\'' +
				", srcTableName='" + srcTableName + '\'' +
				", targetColumnName='" + targetColumnName + '\'' +
				", targetTableName='" + targetTableName + '\'' +
				'}';
	}
}

package org.gama.stalactite.persistence.engine;

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

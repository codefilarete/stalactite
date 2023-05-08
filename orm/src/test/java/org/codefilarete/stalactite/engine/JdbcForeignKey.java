package org.codefilarete.stalactite.engine;

import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

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
	
	public JdbcForeignKey(ForeignKey<? extends Table, ? extends Table, ?> fk) {
		this(fk.getName(),
				fk.getTable().getName(),
				Iterables.first(fk.getColumns()).getName(),
				fk.getTargetTable().getName(),
				Iterables.first(fk.getTargetColumns()).getName());
	}
	
	public String getName() {
		return name;
	}

	public String getSrcColumnName() {
		return srcColumnName;
	}

	public String getSrcTableName() {
		return srcTableName;
	}

	public String getTargetColumnName() {
		return targetColumnName;
	}

	public String getTargetTableName() {
		return targetTableName;
	}
	
	String getSignature() {
		return "JdbcForeignKey{" +
				"name='" + name + '\'' +
				", srcTableName='" + srcTableName + '\'' +
				", srcColumnName='" + srcColumnName + '\'' +
				", targetTableName='" + targetTableName + '\'' +
				", targetColumnName='" + targetColumnName + '\'' +
				'}';
	}
}

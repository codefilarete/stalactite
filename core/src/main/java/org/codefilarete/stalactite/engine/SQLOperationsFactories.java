package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.sql.ddl.DDLTableGenerator;
import org.codefilarete.stalactite.sql.statement.DMLGenerator;
import org.codefilarete.stalactite.sql.statement.ReadOperationFactory;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;

/**
 * @author Guillaume Mary
 */
public class SQLOperationsFactories {
	
	private final WriteOperationFactory writeOperationFactory;
	
	private final ReadOperationFactory readOperationFactory;
	
	private final DMLGenerator dmlGenerator;
	
	private final DDLTableGenerator ddlTableGenerator;
	
	public SQLOperationsFactories(WriteOperationFactory writeOperationFactory,
								  ReadOperationFactory readOperationFactory,
								  DMLGenerator dmlGenerator,
								  DDLTableGenerator ddlTableGenerator) {
		this.writeOperationFactory = writeOperationFactory;
		this.readOperationFactory = readOperationFactory;
		this.dmlGenerator = dmlGenerator;
		this.ddlTableGenerator = ddlTableGenerator;
	}
	
	public WriteOperationFactory getWriteOperationFactory() {
		return writeOperationFactory;
	}
	
	public ReadOperationFactory getReadOperationFactory() {
		return readOperationFactory;
	}
	
	public DMLGenerator getDmlGenerator() {
		return dmlGenerator;
	}
	
	public DDLTableGenerator getDdlTableGenerator() {
		return ddlTableGenerator;
	}
}

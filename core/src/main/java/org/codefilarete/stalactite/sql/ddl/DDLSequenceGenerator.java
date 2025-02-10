package org.codefilarete.stalactite.sql.ddl;

import java.util.Collections;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class DDLSequenceGenerator {
	
	protected final DMLNameProvider dmlNameProvider;
	
	public DDLSequenceGenerator(DMLNameProviderFactory dmlNameProvider) {
		this.dmlNameProvider = dmlNameProvider.build(Collections.emptyMap());
	}
	
	public DDLSequenceGenerator(DMLNameProvider dmlNameProvider) {
		this.dmlNameProvider = dmlNameProvider;
	}
	
	public String generateCreateSequence(Sequence sequence) {
		StringAppender sqlCreateTable = new StringAppender("create sequence ", sequence.getAbsoluteName());
		sqlCreateTable.catIf(sequence.getInitialValue() != null, " start with ", sequence.getInitialValue());
		sqlCreateTable.catIf(sequence.getBatchSize() != null, " increment by ", sequence.getBatchSize());
		return sqlCreateTable.toString();
	}
	
	public String generateDropSequence(Sequence sequence) {
		return "drop sequence " + sequence.getAbsoluteName();
	}
}
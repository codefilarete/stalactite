package org.codefilarete.stalactite.sql.ddl;

import java.util.Collections;
import java.util.HashMap;

import org.codefilarete.stalactite.query.builder.DMLNameProvider;
import org.codefilarete.stalactite.query.builder.QuotingDMLNameProvider;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.sql.ddl.structure.Database;
import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
class DDLSequenceGeneratorTest {
	
	@Test
	public void generateCreateSequence_sequenceHasInitialValueOnly() {
		DDLSequenceGenerator testInstance = new DDLSequenceGenerator(new DMLNameProvider(new HashMap<>()));
		Sequence sequence = new Sequence("my_sequence").withInitialValue(10);
		
		String result = testInstance.generateCreateSequence(sequence);
		
		assertThat(result).isEqualTo("create sequence my_sequence start with 10");
	}
	
	@Test
	public void generateCreateSequence_sequenceHasBatchSizeOnly() {
		DDLSequenceGenerator testInstance = new DDLSequenceGenerator(new DMLNameProvider(new HashMap<>()));
		Sequence sequence = new Sequence("my_sequence").withBatchSize(10);
		
		String result = testInstance.generateCreateSequence(sequence);
		
		assertThat(result).isEqualTo("create sequence my_sequence increment by 10");
	}
	
	@Test
	public void generateCreateSequence_sequenceHasNullValues() {
		DDLSequenceGenerator testInstance = new DDLSequenceGenerator(new DMLNameProvider(new HashMap<>()));
		Sequence sequence = new Sequence("my_sequence");
		sequence.setInitialValue(null);
		sequence.setBatchSize(null);
		
		String result = testInstance.generateCreateSequence(sequence);
		
		assertThat(result).isEqualTo("create sequence my_sequence");
	}
	
	@Test
	public void generateCreateSequence_sequenceHasSchema() {
		DDLSequenceGenerator testInstance = new DDLSequenceGenerator(new DMLNameProvider(new HashMap<>()));
		Sequence sequence = new Sequence(new Database().new Schema("any_schema"), "my_sequence").withInitialValue(10);
		
		String result = testInstance.generateCreateSequence(sequence);
		
		assertThat(result).isEqualTo("create sequence any_schema.my_sequence start with 10");
	}	
	@Test
	public void sequenceScript_nameIsEscapedIfKeyword() {
		DDLSequenceGenerator testInstance = new DDLSequenceGenerator(new QuotingDMLNameProvider(Collections.<Fromable, String>emptyMap()::get, '\''));
		Sequence sequence = new Sequence(new Database().new Schema("any_schema"), "my_sequence");
		
		String result;
		
		result = testInstance.generateCreateSequence(sequence);
		assertThat(result).isEqualTo("create sequence 'any_schema.my_sequence'");
		
		result = testInstance.generateDropSequence(sequence);
		assertThat(result).isEqualTo("drop sequence 'any_schema.my_sequence'");
	}
}

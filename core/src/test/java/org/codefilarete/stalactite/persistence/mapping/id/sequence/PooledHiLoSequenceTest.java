package org.codefilarete.stalactite.persistence.mapping.id.sequence;

import org.codefilarete.stalactite.persistence.mapping.id.sequence.PooledHiLoSequence;
import org.codefilarete.stalactite.persistence.mapping.id.sequence.PooledHiLoSequenceOptions;
import org.codefilarete.stalactite.persistence.mapping.id.sequence.SequencePersister;
import org.codefilarete.stalactite.persistence.mapping.id.sequence.SequenceStorageOptions;
import org.codefilarete.tool.trace.ModifiableInt;
import org.codefilarete.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.sql.ddl.DDLGenerator;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * @author Guillaume Mary
 */
class PooledHiLoSequenceTest {
    
    @Test
    void next_noExistingValueInDatabase() {
        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT);
        SequencePersister sequencePersisterMock = Mockito.mock(SequencePersister.class);
        ModifiableInt sequenceValue = new ModifiableInt();
        when(sequencePersisterMock.select("Toto")).thenReturn(null);
        when(sequencePersisterMock.reservePool("Toto", 10)).thenReturn((long) sequenceValue.increment() * totoSequenceOptions.getPoolSize());
        
        PooledHiLoSequence testInstance = new PooledHiLoSequence(totoSequenceOptions, sequencePersisterMock);
        
        // we check that we can increment from an empty database
        for (int i = 0; i < 45; i++) {    // 45 is totally arbitrary, at least more that poolSize to check that reservePool() is called
            assertThat(testInstance.next().intValue()).isEqualTo(i);
        }
        
        Mockito.verify(sequencePersisterMock, times(5)).reservePool("Toto", 10);
    }
    
    @Test
    void next_existingValueInDatabase() {
        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT);
        SequencePersister sequencePersisterMock = Mockito.mock(SequencePersister.class);
        ModifiableInt sequenceValue = new ModifiableInt(50);
        when(sequencePersisterMock.select("Toto")).thenReturn(new SequencePersister.Sequence("Toto", sequenceValue.getValue()));
        when(sequencePersisterMock.reservePool("Toto", 10)).thenReturn((long) sequenceValue.increment() * totoSequenceOptions.getPoolSize());
        
        PooledHiLoSequence testInstance = new PooledHiLoSequence(totoSequenceOptions, sequencePersisterMock);
		
        for (int i = 0; i < 45; i++) {
            // 50 because previous call to Toto sequence had a pool size of 10, and its last call was 45. So the external state was 50.
            // (upper bound of 45 by step of 10)
            assertThat(testInstance.next().intValue()).isEqualTo(50 + i);
        }
        
        Mockito.verify(sequencePersisterMock, times(5)).reservePool("Toto", 10);
    }
    
    @Test
    void next_withInitialValue() {
        // we check that we can increment a sequence from a different initial value
        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT, -42);
        SequencePersister sequencePersisterMock = Mockito.mock(SequencePersister.class);
        ModifiableInt sequenceValue = new ModifiableInt(50);
        when(sequencePersisterMock.select("Toto")).thenReturn(null);
        when(sequencePersisterMock.reservePool("Toto", 10)).thenReturn((long) sequenceValue.increment() * totoSequenceOptions.getPoolSize());

        PooledHiLoSequence testInstance = new PooledHiLoSequence(totoSequenceOptions, sequencePersisterMock);
        for (int i = -42; i < -25; i++) {
            assertThat(testInstance.next().intValue()).isEqualTo(i);
        }
    }
    
    @Test
    void generateDDL() {
        JavaTypeToSqlTypeMapping simpleTypeMapping = new JavaTypeToSqlTypeMapping();
        simpleTypeMapping.put(long.class, "int");
        simpleTypeMapping.put(String.class, "VARCHAR(255)");

        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", SequenceStorageOptions.DEFAULT);
        Dialect dialect = new Dialect(simpleTypeMapping);
        PooledHiLoSequence testInstance = new PooledHiLoSequence(totoSequenceOptions, dialect, mock(SeparateTransactionExecutor.class), 50);

        // Generating sequence table schema
        DDLGenerator ddlGenerator = new DDLGenerator(dialect.getDdlTableGenerator());
        ddlGenerator.setTables(testInstance.getPersister().giveImpliedTables());
        assertThat(ddlGenerator.getCreationScripts()).containsExactly("create table sequence_table(sequence_name VARCHAR(255), next_val int not null, primary key (sequence_name))");
    }
}
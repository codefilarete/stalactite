package org.codefilarete.stalactite.mapping.id.sequence;

import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequence;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceOptions;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequencePersister;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceStorageOptions;
import org.codefilarete.stalactite.sql.DefaultDialect;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.DDLGenerator;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.tool.trace.ModifiableInt;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class PooledHiLoSequenceTest {
    
    @Test
    void next_noExistingValueInDatabase() {
        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", PooledHiLoSequenceStorageOptions.DEFAULT);
        PooledHiLoSequencePersister sequencePersisterMock = Mockito.mock(PooledHiLoSequencePersister.class);
        ModifiableInt sequenceValue = new ModifiableInt();
        when(sequencePersisterMock.select("Toto")).thenReturn(null);
        when(sequencePersisterMock.reservePool("Toto", 10)).thenReturn((long) sequenceValue.increment() * totoSequenceOptions.getPoolSize());
        
        PooledHiLoSequence testInstance = new PooledHiLoSequence(totoSequenceOptions, sequencePersisterMock);
        
        // we check that we can increment from an empty database
        for (int i = 1; i < 45; i++) {    // 45 is totally arbitrary, at least more that poolSize to check that reservePool() is called
            assertThat(testInstance.next().intValue()).isEqualTo(i);
        }
        
        Mockito.verify(sequencePersisterMock, times(5)).reservePool("Toto", 10);
    }
    
    @Test
    void next_existingValueInDatabase() {
        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", PooledHiLoSequenceStorageOptions.DEFAULT);
        PooledHiLoSequencePersister sequencePersisterMock = Mockito.mock(PooledHiLoSequencePersister.class);
        ModifiableInt sequenceValue = new ModifiableInt(50);
        when(sequencePersisterMock.select("Toto")).thenReturn(new PooledHiLoSequencePersister.Sequence("Toto", sequenceValue.getValue()));
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
        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", PooledHiLoSequenceStorageOptions.DEFAULT, -42);
        PooledHiLoSequencePersister sequencePersisterMock = Mockito.mock(PooledHiLoSequencePersister.class);
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

        PooledHiLoSequenceOptions totoSequenceOptions = new PooledHiLoSequenceOptions(10, "Toto", PooledHiLoSequenceStorageOptions.DEFAULT);
        Dialect dialect = new DefaultDialect(simpleTypeMapping);
        PooledHiLoSequence testInstance = new PooledHiLoSequence(totoSequenceOptions, dialect, mock(SeparateTransactionExecutor.class), 50);

        // Generating sequence table schema
        DDLGenerator ddlGenerator = new DDLGenerator(dialect.getDdlTableGenerator(), dialect.getDdlSequenceGenerator());
        ddlGenerator.setTables(testInstance.getPersister().giveImpliedTables());
        assertThat(ddlGenerator.getCreationScripts()).containsExactly("create table sequence_table(sequence_name VARCHAR(255), next_val int not null, primary key (sequence_name))");
    }
}
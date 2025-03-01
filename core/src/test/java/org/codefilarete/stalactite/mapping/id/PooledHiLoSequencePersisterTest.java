package org.codefilarete.stalactite.mapping.id;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.stalactite.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.engine.SeparateTransactionExecutor.JdbcOperation;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequencePersister;
import org.codefilarete.stalactite.mapping.id.sequence.hilo.PooledHiLoSequenceStorageOptions;
import org.codefilarete.stalactite.sql.DefaultDialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class PooledHiLoSequencePersisterTest {
	
	@Test
	void constructorWithoutArgs_usesDefaultStorageOptions() {
		PooledHiLoSequencePersister testInstance = new PooledHiLoSequencePersister(new DefaultDialect(), mock(SeparateTransactionExecutor.class), 42);
		PooledHiLoSequencePersister.SequenceTable sequenceTable = testInstance.getMapping().getTargetTable();

		// Note that we don't use equals() method because it compares Table partially
		assertThat(sequenceTable.getName()).isEqualTo(PooledHiLoSequenceStorageOptions.DEFAULT.getTable());
		assertThat(sequenceTable.mapColumnsOnName().keySet().size()).isEqualTo(2);
		// checking types : since columns are bound to SequencePersister.Sequence fields, they are of their type
		assertThat(sequenceTable.mapColumnsOnName().get(PooledHiLoSequenceStorageOptions.DEFAULT.getSequenceNameColumn()))
			.extracting(Column::getJavaType).isEqualTo(String.class);
		assertThat(sequenceTable.mapColumnsOnName().get(PooledHiLoSequenceStorageOptions.DEFAULT.getSequenceNameColumn()))
			.extracting(Column::isPrimaryKey).isEqualTo(true);
		assertThat(sequenceTable.mapColumnsOnName().get(PooledHiLoSequenceStorageOptions.DEFAULT.getValueColumn()))
			.extracting(Column::getJavaType).isEqualTo(long.class);
		assertThat(sequenceTable.mapColumnsOnName().get(PooledHiLoSequenceStorageOptions.DEFAULT.getValueColumn()))
			.extracting(Column::isNullable).isEqualTo(false);
	}
	
	@Test
	void constructorWithOptions() {
		PooledHiLoSequenceStorageOptions storageOptions = new PooledHiLoSequenceStorageOptions("myTable", "mySequenceNameCol", "myNextValCol");
		PooledHiLoSequencePersister testInstance = new PooledHiLoSequencePersister(storageOptions, new DefaultDialect(), mock(SeparateTransactionExecutor.class), 42);
		PooledHiLoSequencePersister.SequenceTable sequenceTable = testInstance.getMapping().getTargetTable();

		// Note that we don't use equals() method because it compares Table partially
		assertThat(sequenceTable.getName()).isEqualTo(storageOptions.getTable());
		assertThat(sequenceTable.mapColumnsOnName().keySet().size()).isEqualTo(2);
		// checking types : since columns are bound to SequencePersister.Sequence fields, they are of their type
		assertThat(sequenceTable.mapColumnsOnName().get(storageOptions.getSequenceNameColumn()))
			.extracting(Column::getJavaType).isEqualTo(String.class);
		assertThat(sequenceTable.mapColumnsOnName().get(storageOptions.getSequenceNameColumn()))
			.extracting(Column::isPrimaryKey).isEqualTo(true);
		assertThat(sequenceTable.mapColumnsOnName().get(storageOptions.getValueColumn()))
			.extracting(Column::getJavaType).isEqualTo(long.class);
		assertThat(sequenceTable.mapColumnsOnName().get(storageOptions.getValueColumn()))
			.extracting(Column::isNullable).isEqualTo(false);
	}
	
	@Test
	void reservePool_emptyDatabase() {
		List<String> selectedSequence = new ArrayList<>();
		List<PooledHiLoSequencePersister.Sequence> insertedSequence = new ArrayList<>();
		List<PooledHiLoSequencePersister.Sequence> updatedSequence = new ArrayList<>();
		SeparateTransactionExecutor separateTransactionExecutorMock = mock(SeparateTransactionExecutor.class);
		doAnswer((Answer<Void>) invocationOnMock -> {
			((JdbcOperation) invocationOnMock.getArgument(0)).execute(null);
			return null;
		}).when(separateTransactionExecutorMock).executeInNewTransaction(any());
		PooledHiLoSequencePersister testInstance = new PooledHiLoSequencePersister(new DefaultDialect(), separateTransactionExecutorMock, 42) {
			
			@Override
			public Sequence select(String sequenceName) {
				selectedSequence.add(sequenceName);
				return null;
			}

			@Override
			public void insert(Sequence entity) {
				insertedSequence.add(entity);
			}

			@Override
			public void updateById(Sequence entity) {
				updatedSequence.add(entity);
			}
		};
		long identifier = testInstance.reservePool("toto", 10);
		assertThat(identifier).isEqualTo(10);
		assertThat(selectedSequence).isEqualTo(Arrays.asList("toto"));
		assertThat(insertedSequence).flatExtracting(PooledHiLoSequencePersister.Sequence::getSequenceName).isEqualTo(Arrays.asList("toto"));
		assertThat(insertedSequence).flatExtracting(PooledHiLoSequencePersister.Sequence::getStep).isEqualTo(Arrays.asList(10L));
		assertThat(updatedSequence).isEmpty();
	}

	@Test
	void reservePool_notEmptyDatabase() {
		List<String> selectedSequence = new ArrayList<>();
		List<PooledHiLoSequencePersister.Sequence> insertedSequence = new ArrayList<>();
		List<PooledHiLoSequencePersister.Sequence> updatedSequence = new ArrayList<>();
		SeparateTransactionExecutor separateTransactionExecutorMock = mock(SeparateTransactionExecutor.class);
		doAnswer((Answer<Void>) invocationOnMock -> {
			((JdbcOperation) invocationOnMock.getArgument(0)).execute(null);
			return null;
		}).when(separateTransactionExecutorMock).executeInNewTransaction(any());
		PooledHiLoSequencePersister testInstance = new PooledHiLoSequencePersister(new DefaultDialect(), separateTransactionExecutorMock, 42) {

			@Override
			public Sequence select(String sequenceName) {
				selectedSequence.add(sequenceName);
				return new Sequence(sequenceName, 15);
			}

			@Override
			public void insert(Sequence entity) {
				insertedSequence.add(entity);
			}

			@Override
			public void updateById(Sequence entity) {
				updatedSequence.add(entity);
			}
		};
		long identifier = testInstance.reservePool("toto", 10);
		assertThat(identifier).isEqualTo(25);
		assertThat(selectedSequence).isEqualTo(Arrays.asList("toto"));
		assertThat(insertedSequence).isEmpty();
		assertThat(updatedSequence).flatExtracting(PooledHiLoSequencePersister.Sequence::getSequenceName).isEqualTo(Arrays.asList("toto"));
		assertThat(updatedSequence).flatExtracting(PooledHiLoSequencePersister.Sequence::getStep).isEqualTo(Arrays.asList(25L));
	}
}
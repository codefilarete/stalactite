package org.codefilarete.stalactite.persistence.id;

import java.util.ArrayList;
import java.util.List;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.codefilarete.stalactite.persistence.id.sequence.SequencePersister;
import org.codefilarete.stalactite.persistence.id.sequence.SequenceStorageOptions;
import org.codefilarete.stalactite.persistence.sql.Dialect;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SequencePersisterTest {
	
	@Test
	void constructorWithoutArgs_usesDefaultStorageOptions() {
		SequencePersister testInstance = new SequencePersister(new Dialect(), mock(SeparateTransactionExecutor.class), 42);
		SequencePersister.SequenceTable sequenceTable = testInstance.getMappingStrategy().getTargetTable();

		// Note that we don't use equals() method because it compares Table partially
		assertThat(sequenceTable.getName()).isEqualTo(SequenceStorageOptions.DEFAULT.getTable());
		assertThat(sequenceTable.mapColumnsOnName().keySet().size()).isEqualTo(2);
		// checking types : since columns are bound to SequencePersister.Sequence fields, they are of their type
		assertThat(sequenceTable.mapColumnsOnName().get(SequenceStorageOptions.DEFAULT.getSequenceNameColumn()))
			.extracting(Column::getJavaType).isEqualTo(String.class);
		assertThat(sequenceTable.mapColumnsOnName().get(SequenceStorageOptions.DEFAULT.getSequenceNameColumn()))
			.extracting(Column::isPrimaryKey).isEqualTo(true);
		assertThat(sequenceTable.mapColumnsOnName().get(SequenceStorageOptions.DEFAULT.getValueColumn()))
			.extracting(Column::getJavaType).isEqualTo(long.class);
		assertThat(sequenceTable.mapColumnsOnName().get(SequenceStorageOptions.DEFAULT.getValueColumn()))
			.extracting(Column::isNullable).isEqualTo(false);
	}
	
	@Test
	void constructorWithOptions() {
		SequenceStorageOptions storageOptions = new SequenceStorageOptions("myTable", "mySequenceNameCol", "myNextValCol");
		SequencePersister testInstance = new SequencePersister(storageOptions, new Dialect(), mock(SeparateTransactionExecutor.class), 42);
		SequencePersister.SequenceTable sequenceTable = testInstance.getMappingStrategy().getTargetTable();

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
		List<SequencePersister.Sequence> insertedSequence = new ArrayList<>();
		List<SequencePersister.Sequence> updatedSequence = new ArrayList<>();
		SeparateTransactionExecutor separateTransactionExecutorMock = mock(SeparateTransactionExecutor.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				((SeparateTransactionExecutor.JdbcOperation) invocationOnMock.getArgument(0)).execute();
				return null;
			}
		}).when(separateTransactionExecutorMock).executeInNewTransaction(any());
		SequencePersister testInstance = new SequencePersister(new Dialect(), separateTransactionExecutorMock, 42) {
			
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
		assertThat(insertedSequence).flatExtracting(SequencePersister.Sequence::getSequenceName).isEqualTo(Arrays.asList("toto"));
		assertThat(insertedSequence).flatExtracting(SequencePersister.Sequence::getStep).isEqualTo(Arrays.asList(10L));
		assertThat(updatedSequence).isEmpty();
	}

	@Test
	void reservePool_notEmptyDatabase() {
		List<String> selectedSequence = new ArrayList<>();
		List<SequencePersister.Sequence> insertedSequence = new ArrayList<>();
		List<SequencePersister.Sequence> updatedSequence = new ArrayList<>();
		SeparateTransactionExecutor separateTransactionExecutorMock = mock(SeparateTransactionExecutor.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				((SeparateTransactionExecutor.JdbcOperation) invocationOnMock.getArgument(0)).execute();
				return null;
			}
		}).when(separateTransactionExecutorMock).executeInNewTransaction(any());
		SequencePersister testInstance = new SequencePersister(new Dialect(), separateTransactionExecutorMock, 42) {

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
		assertThat(updatedSequence).flatExtracting(SequencePersister.Sequence::getSequenceName).isEqualTo(Arrays.asList("toto"));
		assertThat(updatedSequence).flatExtracting(SequencePersister.Sequence::getStep).isEqualTo(Arrays.asList(25L));
	}
}
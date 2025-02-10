package org.codefilarete.stalactite.mapping.id.sequence;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.codefilarete.stalactite.sql.ddl.structure.Sequence;
import org.codefilarete.tool.trace.ModifiableLong;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DatabaseSequenceSelectorTest {
	
	@Test
	void next_basic() {
		List<Long> databaseCalls = new ArrayList<>();
		ModifiableLong counter = new ModifiableLong();
		DatabaseSequenceSelector testInstance = new DatabaseSequenceSelector(new Sequence("whatever sequence name"), "whatever SQL, it won't be called", null) {
			@Override
			long callDatabase() {
				long value = counter.increment();
				databaseCalls.add(value);
				return value;
			}
		};
		
		assertThat(testInstance.next()).isEqualTo(1L);
		assertThat(testInstance.next()).isEqualTo(2L);
		assertThat(testInstance.next()).isEqualTo(3L);
		assertThat(databaseCalls).containsExactly(1L, 2L, 3L);
	}
	
	@Test
	void next_withPoolSize() {
		List<Long> databaseCalls = new ArrayList<>();
		int poolSize = 3;
		ModifiableLong counter = new ModifiableLong(-poolSize + 1);
		DatabaseSequenceSelector testInstance = new DatabaseSequenceSelector(new Sequence("whatever sequence name")
				.withBatchSize(poolSize), "whatever SQL, it won't be called", null) {
			@Override
			long callDatabase() {
				long value = counter.increment(poolSize);
				databaseCalls.add(value);
				return value;
			}
		};
		
		for (int i = 1; i < 10; i++) {
			Long sequenceValue = testInstance.next();
			assertThat(sequenceValue).isEqualTo(i);
		}
		assertThat(databaseCalls).containsExactly(1L, 4L, 7L);
	}
	
	@Test
	void next_withPoolSize_multiThread() throws InterruptedException {
		List<Long> databaseCalls = new ArrayList<>();
		int poolSize = 3;
		AtomicLong counter = new AtomicLong(1);
		DatabaseSequenceSelector testInstance = new DatabaseSequenceSelector(new Sequence("whatever sequence name")
				.withBatchSize(poolSize), "whatever SQL, it won't be called", null) {
			@Override
			long callDatabase() {
				long value = counter.getAndAdd(poolSize);
				databaseCalls.add(value);
				return value;
			}
		};
		
		List<Long> generatedValues = Collections.synchronizedList(new ArrayList<>());
		ExecutorService workerPool = Executors.newFixedThreadPool(3);
		for (int i = 0; i < 20; i++) {
			workerPool.submit(() -> {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				generatedValues.add(testInstance.next());
			});
		}
		
		Thread.sleep(500);
		
		assertThat(generatedValues).contains(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
		assertThat(databaseCalls).contains(1L, 4L, 7L);
		workerPool.shutdown();
	}
}
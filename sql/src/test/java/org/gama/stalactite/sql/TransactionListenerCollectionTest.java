package org.gama.stalactite.sql;

import java.sql.Savepoint;

import org.gama.lang.trace.ModifiableInt;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Guillaume Mary
 */
public class TransactionListenerCollectionTest {
	
	@Test
	public void testAfterCommit_temporaryListenersShouldNotBeCalledTwice() {
		TransactionListenerCollection testInstance = new TransactionListenerCollection();
		MutableBoolean isTemporary = new MutableBoolean();
		ModifiableInt modifiableInt = new ModifiableInt();
		CommitListener temporaryCommitListener = new CommitListener() {
			@Override
			public void beforeCommit() {
			}
			
			@Override
			public void afterCommit() {
				modifiableInt.increment();
			}
			
			@Override
			public boolean isTemporary() {
				return isTemporary.booleanValue();
			}
		};
		testInstance.addCommitListener(temporaryCommitListener);
		
		// With a non-temporary listener, our counter will be incremented as we call afterCommit()
		isTemporary.setFalse();
		testInstance.afterCommit();
		assertThat(modifiableInt.getValue()).isEqualTo(1);
		// A second call to afterCommit() will still increment our counter
		testInstance.afterCommit();
		assertThat(modifiableInt.getValue()).isEqualTo(2);
		
		// But if the listener is set as temporary then only next call to afterCommit() will be effective
		isTemporary.setTrue();
		testInstance.afterCommit();
		assertThat(modifiableInt.getValue()).isEqualTo(3);
		// no more increment
		testInstance.afterCommit();
		assertThat(modifiableInt.getValue()).isEqualTo(3);
	}
	
	@Test
	public void testAfterRolback_temporaryListenersShouldNotBeCalledTwice() {
		TransactionListenerCollection testInstance = new TransactionListenerCollection();
		MutableBoolean isTemporary = new MutableBoolean();
		ModifiableInt modifiableInt = new ModifiableInt();
		RollbackListener temporaryRollbackListener = new RollbackListener() {
			
			@Override
			public void beforeRollback() {
				
			}
			
			@Override
			public void afterRollback() {
				modifiableInt.increment();
			}
			
			@Override
			public void beforeRollback(Savepoint savepoint) {
				
			}
			
			@Override
			public void afterRollback(Savepoint savepoint) {
				
			}
			
			@Override
			public boolean isTemporary() {
				return isTemporary.booleanValue();
			}
		};
		testInstance.addRollbackListener(temporaryRollbackListener);
		
		// With a non-temporary listener, our counter will be incremented as we call afterRollback()
		isTemporary.setFalse();
		testInstance.afterRollback();
		assertThat(modifiableInt.getValue()).isEqualTo(1);
		// A second call to afterRollback() will still increment our counter
		testInstance.afterRollback();
		assertThat(modifiableInt.getValue()).isEqualTo(2);
		
		// But if the listener is set as temporary then only next call to afterRollback() will be effective
		isTemporary.setTrue();
		testInstance.afterRollback();
		assertThat(modifiableInt.getValue()).isEqualTo(3);
		// no more increment
		testInstance.afterRollback();
		assertThat(modifiableInt.getValue()).isEqualTo(3);
	}
	
	private static class MutableBoolean {
		
		private boolean value;
		
		public boolean booleanValue() {
			return value;
		}
		
		public void setTrue() {
			this.value = true;
		}
		
		public void setFalse() {
			this.value = false;
		}
	}
}
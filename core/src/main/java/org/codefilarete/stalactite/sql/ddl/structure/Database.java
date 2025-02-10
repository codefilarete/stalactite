package org.codefilarete.stalactite.sql.ddl.structure;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

/**
 * @author mary
 */
public class Database {
	
	public class Schema {
		
		@Nullable
		private final String name;
		
		private final Set<Table<?>> tables = new HashSet<>();
		
		private final Set<Sequence> sequences = new HashSet<>();
		
		public Schema() {
			this(null);
		}
		
		public Schema(@Nullable String name) {
			this.name = name;
		}
		
		@Nullable
		public String getName() {
			return name;
		}
		
		public Database getDatabase() {
			return Database.this;
		}
		
		public Set<Table<?>> getTables() {
			return tables;
		}
		
		void addTable(Table<?> table) {
			this.tables.add(table);
		}
		
		public Set<Sequence> getSequences() {
			return sequences;
		}
		
		public void addSequence(Sequence sequence) {
			this.sequences.add(sequence);
		}
	}
}

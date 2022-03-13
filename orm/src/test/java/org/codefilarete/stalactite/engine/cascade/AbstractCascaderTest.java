package org.codefilarete.stalactite.engine.cascade;

/**
 * @author Guillaume Mary
 */
public abstract class AbstractCascaderTest {
	
	protected static class Toto {
		
		protected Tata tata;
		
		protected Toto() {
		}
		
		protected Toto(Tata tata) {
			this.tata = tata;
		}
	}
	
	protected static class Tata {
		
		protected Long id;
		
		private String name;
		
		protected Tata() {
		}
		
		public Tata setName(String name) {
			this.name = name;
			return this;
		}
		
		public Tata setId(long id) {
			this.id = id;
			return this;
		}
	}
}
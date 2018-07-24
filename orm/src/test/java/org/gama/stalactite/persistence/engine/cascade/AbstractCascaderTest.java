package org.gama.stalactite.persistence.engine.cascade;

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
	}
}
package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.engine.runtime.DMLExecutorTest.Toto;

class VersionnedToto extends Toto {
	
	protected long version;
	
	public VersionnedToto(int a, int b, int c) {
		super(a, b, c);
	}
	
	public Integer getA() {
		return a;
	}
	
	public void setA(Integer a) {
		this.a = a;
	}
	
	public long getVersion() {
		return version;
	}
	
	public void setVersion(long version) {
		this.version = version;
	}
}

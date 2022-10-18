package org.codefilarete.stalactite.sql.test;

import java.io.PrintWriter;
import java.io.Writer;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.bean.Randomizer;

/**
 * Simple Derby DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class DerbyInMemoryDataSource extends UrlAwareDataSource {
	
	/** No operation logger to get rid of derby.log, see System.setProperty("derby.stream.error.field"), must be public */
	public static final PrintWriter NOOP_LOGGER = new PrintWriter(new NoopWriter());
	
	public DerbyInMemoryDataSource() {
		// random URL to avoid conflict between tests
		this(Randomizer.INSTANCE.randomHexString(8));
	}
	
	private DerbyInMemoryDataSource(String databaseName) {
		super("jdbc:derby:memory:"+ databaseName);
		EmbeddedDataSource delegate = new EmbeddedDataSource();
		delegate.setDatabaseName("memory:" + databaseName);
		// we enable the creation of the schema
		delegate.setCreateDatabase("create");
		fixDerbyLogger();
		setDelegate(delegate);
	}
	
	/**
	 * Implentation that disables logs (prevent derby.log polluting project build).
	 * Left protected to change this behavior.
	 */
	protected void fixDerbyLogger() {
		// get rid of default derby.log (setLogWriter doesn't work)
		System.setProperty(Property.ERRORLOG_FIELD_PROPERTY, DerbyInMemoryDataSource.class.getName() + ".NOOP_LOGGER");
	}
	
	/**
	 * A writer that does nothing
	 */
	private static class NoopWriter extends Writer {
		@Override
		public void write(char[] cbuf, int off, int len) {
			// nothing to do because of goal's class
		}
		
		@Override
		public void flush() {
			// nothing to do because of goal's class
		}
		
		@Override
		public void close() {
			// nothing to do because of goal's class
		}
	}
}

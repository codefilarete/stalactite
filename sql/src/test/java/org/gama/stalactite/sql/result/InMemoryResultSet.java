package org.gama.stalactite.sql.result;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link java.sql.ResultSet} implementation based on a in-memory structure that is filled at construction time
 * 
 * @author Guillaume Mary
 */
public class InMemoryResultSet extends NoopResultSet {
	
	private final Iterator<Map<String, Object>> data;
	
	private Map<String, Object> currentRow;
	
	public InMemoryResultSet(Iterable<? extends Map<? extends String, ? extends Object>> data) {
		this(data.iterator());
	}
	
	public InMemoryResultSet(Iterator<? extends Map<? extends String, ? extends Object>> data) {
		this.data = (Iterator<Map<String, Object>>) data;
	}
	
	@Override
	public boolean next() throws SQLException {
		boolean hasNext = data.hasNext();
		if (hasNext) {
			currentRow = data.next();
		}
		return hasNext;
	}
	
	@Override
	public boolean wasNull() throws SQLException {
		throw new UnsupportedOperationException();	// to be supported ... but not yet
	}
	
	private void assertExists(String columnName) throws SQLException {
		if (!currentRow.containsKey(columnName)) {
			throw new SQLException("Column doesn't exist : " + columnName);
		}
	}
	
	@Override
	public String getString(String columnName) throws SQLException {
		assertExists(columnName);
		return (String) currentRow.get(columnName);
	}
	
	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		assertExists(columnName);
		return (boolean) currentRow.get(columnName);
	}
	
	@Override
	public byte getByte(String columnName) throws SQLException {
		assertExists(columnName);
		return (byte) currentRow.get(columnName);
	}
	
	@Override
	public short getShort(String columnName) throws SQLException {
		assertExists(columnName);
		return (short) currentRow.get(columnName);
	}
	
	@Override
	public int getInt(String columnName) throws SQLException {
		assertExists(columnName);
		return (int) currentRow.get(columnName);
	}
	
	@Override
	public long getLong(String columnName) throws SQLException {
		assertExists(columnName);
		return (long) currentRow.get(columnName);
	}
	
	@Override
	public float getFloat(String columnName) throws SQLException {
		assertExists(columnName);
		return (float) currentRow.get(columnName);
	}
	
	@Override
	public double getDouble(String columnName) throws SQLException {
		assertExists(columnName);
		return (double) currentRow.get(columnName);
	}
	
	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		assertExists(columnName);
		return (byte[]) currentRow.get(columnName);
	}
	
	@Override
	public Date getDate(String columnName) throws SQLException {
		assertExists(columnName);
		return (Date) currentRow.get(columnName);
	}
	
	@Override
	public Time getTime(String columnName) throws SQLException {
		assertExists(columnName);
		return (Time) currentRow.get(columnName);
	}
	
	@Override
	public Timestamp getTimestamp(String columnName) throws SQLException {
		assertExists(columnName);
		return (Timestamp) currentRow.get(columnName);
	}
	
	@Override
	public InputStream getAsciiStream(String columnName) throws SQLException {
		assertExists(columnName);
		return (InputStream) currentRow.get(columnName);
	}
	
	@Override
	public InputStream getUnicodeStream(String columnName) throws SQLException {
		assertExists(columnName);
		return (InputStream) currentRow.get(columnName);
	}
	
	@Override
	public InputStream getBinaryStream(String columnName) throws SQLException {
		assertExists(columnName);
		return (InputStream) currentRow.get(columnName);
	}
	
	@Override
	public Object getObject(String columnName) throws SQLException {
		assertExists(columnName);
		return currentRow.get(columnName);
	}
	
	@Override
	public Reader getCharacterStream(String columnName) throws SQLException {
		assertExists(columnName);
		return (Reader) currentRow.get(columnName);
	}
	
	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		assertExists(columnName);
		return (BigDecimal) currentRow.get(columnName);
	}
}

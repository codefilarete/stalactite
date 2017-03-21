package org.gama.sql.result;

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
	
	public InMemoryResultSet(Iterable<Map<String, Object>> data) {
		this(data.iterator());
	}
	
	public InMemoryResultSet(Iterator<Map<String, Object>> data) {
		this.data = data;
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
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		return (String) currentRow.get(columnLabel);
	}
	
	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return (boolean) currentRow.get(columnLabel);
	}
	
	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return (byte) currentRow.get(columnLabel);
	}
	
	@Override
	public short getShort(String columnLabel) throws SQLException {
		return (short) currentRow.get(columnLabel);
	}
	
	@Override
	public int getInt(String columnLabel) throws SQLException {
		return (int) currentRow.get(columnLabel);
	}
	
	@Override
	public long getLong(String columnLabel) throws SQLException {
		return (long) currentRow.get(columnLabel);
	}
	
	@Override
	public float getFloat(String columnLabel) throws SQLException {
		return (float) currentRow.get(columnLabel);
	}
	
	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return (double) currentRow.get(columnLabel);
	}
	
	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return (byte[]) currentRow.get(columnLabel);
	}
	
	@Override
	public Date getDate(String columnLabel) throws SQLException {
		return (Date) currentRow.get(columnLabel);
	}
	
	@Override
	public Time getTime(String columnLabel) throws SQLException {
		return (Time) currentRow.get(columnLabel);
	}
	
	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		return (Timestamp) currentRow.get(columnLabel);
	}
	
	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		return (InputStream) currentRow.get(columnLabel);
	}
	
	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		return (InputStream) currentRow.get(columnLabel);
	}
	
	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		return (InputStream) currentRow.get(columnLabel);
	}
	
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return currentRow.get(columnLabel);
	}
	
	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		return (Reader) currentRow.get(columnLabel);
	}
	
	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return (BigDecimal) currentRow.get(columnLabel);
	}
}

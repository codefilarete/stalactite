package org.gama.stalactite.benchmark;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.gama.lang.bean.Objects;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public class DataGenerator {
	
	private final static String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras vel magna metus." +
			" Quisque ullamcorper vel orci eu ornare. Etiam dapibus rhoncus condimentum. Proin scelerisque mauris consequat rutrum rhoncus." +
			" Nunc porttitor dui id quam accumsan, dignissim feugiat purus sagittis. Vestibulum tortor quam, sagittis eget magna quis, convallis mattis neque." +
			" Cras porttitor sodales euismod. Phasellus eleifend euismod bibendum. Donec congue felis vitae lobortis elementum." +
			" Sed pretium felis id lorem sollicitudin mattis. Pellentesque id sagittis urna. Vestibulum in dolor non eros posuere posuere." +
			" Nunc nec erat consequat, venenatis ipsum sed, congue nisi. Morbi vitae viverra dui. Maecenas at finibus libero." +
			" Ut commodo sit amet ipsum ut euismod.";

	private Random randomizer = new Random();
	
	private final Collection<? extends Column> columns;
	
	public DataGenerator(Collection<? extends Column> columns) {
		this.columns = columns;
	}
	
	public Map<Column, Object> generateData() {
		Map<Column, Object> row = new HashMap<>(columns.size());
		for (Column column : this.columns) {
			Object value = null;
			row.put(column, generateData(column));
			row.put(column, value);
		}
		return row;
	}
	
	protected Object generateData(Column column) {
		if (column.isPrimaryKey()) {
			return generatePrimaryKey(column);
		} else {
			Class javaType = column.getJavaType();
			if (CharSequence.class.isAssignableFrom(javaType)) {
				return randomText(column);
			} else if (Date.class.isAssignableFrom(javaType)) {
				return randomDate(column);
			} else if (Long.class.isAssignableFrom(javaType)) {
				return randomLong(column);
			} else if (Integer.class.isAssignableFrom(javaType)) {
				return randomInteger(column);
			} else if (Double.class.isAssignableFrom(javaType)) {
				return randomDouble(column);
			} else if (Float.class.isAssignableFrom(javaType)) {
				return randomFloat(column);
			} else {
				throw new IllegalArgumentException("Generation data for " + column.getAbsoluteName() + " is not implemented");
			}
		}
	}
	
	protected Object generatePrimaryKey(Column column) {
		return null;
	}
	
	protected String randomText(Column column) {
		int maxLength = Objects.preventNull(column.getSize(), 100);
		return randomText(maxLength);
	}
	
	protected String randomText(int maxLength) {
		int loremIpsumLength = LOREM_IPSUM.length();
		int length = Math.min(loremIpsumLength, randomizer.nextInt(maxLength));
		int start = randomizer.nextInt(loremIpsumLength);
		int end = Math.min(loremIpsumLength, start+length);
		return LOREM_IPSUM.substring(start, end);
	}
	
	protected Date randomDate(Column column) {
		return new Date();
	}
	
	protected Double randomDouble(Column column) {
		return randomizer.nextDouble();
	}
	
	protected Float randomFloat(Column column) {
		return randomizer.nextFloat();
	}
	
	protected Long randomLong(Column column) {
		return randomizer.nextLong();
	}
	
	protected Integer randomInteger(Column column) {
		return randomizer.nextInt();
	}
	
	protected int randomInteger(int n) {
		return randomizer.nextInt(n);
	}
}

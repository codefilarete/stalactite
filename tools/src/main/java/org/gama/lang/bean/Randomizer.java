package org.gama.lang.bean;

import org.gama.lang.collection.Iterables;

import java.util.*;

/**
 * @author Guillaume Mary
 */
public class Randomizer {
	
	public static final Randomizer INSTANCE = new Randomizer();
	
	private static final String BASE64CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
	
	private static final String HEXCHARS = "ABCDEFGH0123456789";

	private final IRandomGenerator random;
	
	public Randomizer() {
		this(new LinearRandomGenerator());
	}
	
	public Randomizer(boolean gaussian) {
		this(gaussian ? new LinearRandomGenerator() : new LinearRandomGenerator());
	}
	
	private Randomizer(IRandomGenerator random) {
		this.random = random;
	}
	
	public double drawDouble() {
		return this.random.randomDouble();
	}
	
	public double drawDouble(double lowBound, double highBound) {
		return (highBound -lowBound) * drawDouble() + lowBound;
	}
	
	public long drawLong(long lowBound, long highBound) {
		return (long) ((highBound -lowBound) * drawDouble() + lowBound);
	}
	
	public int drawInt(int lowBound, int highBound) {
		return (int) ((highBound -lowBound) * drawDouble() + lowBound);
	}
	
	public boolean drawBoolean() {
		return drawDouble() > 0.5;
	}
	
	public Date drawDate(Date lowBound, Date highBound) {
		return new Date(drawLong(lowBound.getTime(), highBound.getTime()));
	}
	
	public String drawString(String hat, int maxLength) {
		int hatLength = hat.length();
		int startIndex = drawInt(0, hatLength);
		int length = drawInt(0, maxLength);
		return hat.substring(startIndex, Math.min(startIndex+length, hatLength));
	}
	
	public <E> List<E> drawElements(Iterable<E> hat, int count) {
		List<E> toReturn = new ArrayList<>(count);
		Iterator<E> hatIterator = hat.iterator();
		while(toReturn.size() < count && hatIterator.hasNext()) {
			if (drawBoolean()) {
				toReturn.add(hatIterator.next());
			}
		}
		return toReturn;
	}
	
	public <E> List<E> drawElements(Collection<E> hat, int count) {
		int hatSize = hat.size();
		// Anti overflow
		count = Math.min(hatSize, count);
		
		List<E> toReturn = new ArrayList<>(count);
		Set<Integer> drawnIndexes = new HashSet<>();
		while (drawnIndexes.size() < count) {
			int drawnIndex;
			do {
				drawnIndex = drawInt(0, hatSize);
			} while(drawnIndexes.contains(drawnIndex));
			drawnIndexes.add(drawnIndex);
		}
		toReturn.addAll(getElementsByIndex(hat, new TreeSet<>(drawnIndexes)));
		return toReturn;
	}
	
	static <E> List<E> getElementsByIndex(Iterable<E> iterable, TreeSet<Integer> indexes) {
		List<E> toReturn;
		if (iterable instanceof List) {
			toReturn = new ArrayList<>(((List) iterable).size());
			for (Integer index : indexes) {
				toReturn.add(((List<E>) iterable).get(index));
			}
		} else {
			toReturn = new ArrayList<>();
			int i = 0;
			Iterator<E> iterator = iterable.iterator();
			for (int index : indexes) {
				E next;
				do {
					next = iterator.next();
					i++;
				} while(i <= index && iterator.hasNext());
				toReturn.add(next);
			}
		}
		return toReturn;
	}
	
	public <E> E drawElement(List<E> hat) {
		int hatSize = hat.size();
		if (hatSize < 2) {
			return Iterables.first(hat);
		} else {
			int drawnIndex = drawInt(0, hatSize);
			return hat.get(drawnIndex);
		}
	}
	
	public char drawChar(String hat) {
		return hat.charAt(drawInt(0, hat.length()));
	}
	
	public String randomHexString(int length) {
		return randomHexString(length, HEXCHARS);
	}
	
	public String randomBase64String(int length) {
		return randomHexString(length, BASE64CHARS);
	}
	
	private String randomHexString(int length, String hat) {
		StringBuilder randomHexString = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			randomHexString.append(drawChar(hat));
		}
		return randomHexString.toString();
	}
	
	
	private interface IRandomGenerator {
		double randomDouble();
	}
	
	private static class LinearRandomGenerator implements IRandomGenerator {
		private Random random = new Random();
		
		@Override
		public double randomDouble() {
			return random.nextDouble();
		}
	}
	
	private static class GaussianRandomGenerator implements IRandomGenerator {
		private Random random = new Random();
		
		@Override
		public double randomDouble() {
			return random.nextGaussian();
		}
	}
}

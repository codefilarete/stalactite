package org.gama.lang.bean;

import org.gama.lang.collection.Iterables;

import java.util.*;

/**
 * @author Guillaume Mary
 */
public class Randomizer {
	
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
	
	public double randomDouble() {
		return this.random.randomDouble();
	}
	
	public double randomDouble(double lowBound, double highBound) {
		return (highBound -lowBound) * randomDouble() + lowBound;
	}
	
	public long randomLong(long lowBound, long highBound) {
		return (long) ((highBound -lowBound) * randomDouble() + lowBound);
	}
	
	public int randomInt(int lowBound, int highBound) {
		return (int) ((highBound -lowBound) * randomDouble() + lowBound);
	}
	
	public boolean randomBoolean() {
		return randomDouble() > 0.5;
	}
	
	public Date randomDate(Date lowBound, Date highBound) {
		return new Date(randomLong(lowBound.getTime(), highBound.getTime()));
	}
	
	public String randomString(String hat, int maxLength) {
		int hatLength = hat.length();
		int startIndex = randomInt(0, hatLength);
		int length = randomInt(0, maxLength);
		return hat.substring(startIndex, Math.min(startIndex+length, hatLength));
	}
	
	public <E> List<E> randomElements(Iterable<E> hat, int count) {
		List<E> toReturn = new ArrayList<>(count);
		Iterator<E> hatIterator = hat.iterator();
		while(toReturn.size() < count && hatIterator.hasNext()) {
			if (randomBoolean()) {
				toReturn.add(hatIterator.next());
			}
		}
		return toReturn;
	}
	
	public <E> List<E> randomElements(List<E> hat, int count) {
		int hatSize = hat.size();
		if (count >= hatSize) {
			return new ArrayList<>(hat);
		} else {
			List<E> toReturn = new ArrayList<>(count);
			Set<Integer> drawnIndexes = new HashSet<>();
			while (toReturn.size() < count) {
				int drawnIndex;
				do {
					drawnIndex = randomInt(0, hatSize);
				} while(drawnIndexes.contains(drawnIndex));
				drawnIndexes.add(drawnIndex);
				toReturn.add(hat.get(drawnIndex));
			}
			return toReturn;
		}
	}
	
	public <E> E randomElement(List<E> hat) {
		int hatSize = hat.size();
		if (hatSize < 2) {
			return Iterables.first(hat);
		} else {
			int drawnIndex = randomInt(0, hatSize);
			return hat.get(drawnIndex);
		}
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

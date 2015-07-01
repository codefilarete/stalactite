package org.gama.lang.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author Guillaume Mary
 */
public class Collections {
	
	/**
	 * Renvoie true si la collection est null ou vide
	 * 
	 * @param c
	 * @return true si la collection est null ou vide
	 */
	public static boolean isEmpty(Collection c) {
		return c == null || c.isEmpty();
	}

	/**
	 * Renvoie true si la Map est null ou vide
	 *
	 * @param m
	 * @return true si la Map est null ou vide
	 */
	public static boolean isEmpty(Map m) {
		return m == null || m.isEmpty();
	}

	public static <E> List<E> cat(Collection<E> ... collections) {
		List<E> toReturn = new ArrayList<>(collections.length * 10);	// arbitrary size, ArrayList.addAll will adapt
		for (Collection<E> collection : collections) {
			toReturn.addAll(collection);
		}
		return toReturn;
	}
	
	public static <E> List<E> cutHead(List<E> list) {
		return new ArrayList<>(list.subList(1, list.size()));
	}
	
	public static <E> List<E> cutTail(List<E> list) {
		return new ArrayList<>(list.subList(0, list.size()-1));
	}
	
	/**
	 * Découpe une collection en paquets
	 * 
	 * @param data
	 * @param blockSize
	 * @return une collection en paquets
	 */
	public static <E> List<List<E>> parcel(Iterable<E> data, int blockSize) {
		final List<List<E>> blocks = new ArrayList<>();
		// on s'assure d'avoir une liste pour permettre l'utilisation de subList ensuite
		List<E> dataAsList;
		if (data instanceof List) {
			dataAsList = (List<E>) data;
		} else {
			dataAsList = Iterables.copy(data);
		}
		int i = 0;
		int dataSize = dataAsList.size();
		int blockCount = dataSize / blockSize;
		// si reliquat, alors un bloc en plus
		if (dataSize % blockSize != 0) {
			blockCount++;
		}
		// découpage
		while(i < blockCount) {
			blocks.add(dataAsList.subList(i*blockSize, Math.min(dataSize, ++i*blockSize)));
		}
		return blocks;
	}
}

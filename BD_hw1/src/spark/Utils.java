package spark;

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import scala.Tuple2;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;

public class Utils {

	/**
	 * Will return combinations of all sizes...
	 * If elements = { a, b, c }, then findCollections(elements) 
	 * will return all unique combinations of elements as:
	 *
	 *    { [], [a], [b], [c], [a, b], [a, c], [b, c], [a, b, c] }
	 *
	 * @param <T>
	 * @param elements a collection of type T elements
	 * @return unique combinations of elements
	 */
	public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements) {
		List<List<T>> result = new ArrayList<List<T>>();
		for (int i = 0; i <= elements.size(); i++) {
			result.addAll(findSortedCombinations(elements, i));
		}
		return result;
	}


	/**
	 * Will return unique combinations of size=n.
	 * If elements = { a, b, c }, then findCollections(elements, 2) will return:
	 *
	 *     { [a, b], [a, c], [b, c] }
	 *
	 * @param <T>
	 * @param elements a collection of type T elements
	 * @param n size of combinations
	 * @return unique combinations of elements of size = n
	 *
	 */
	public static <T extends Comparable<? super T>> List<List<T>> findSortedCombinations(Collection<T> elements, int n) {
		List<List<T>> result = new ArrayList<List<T>>();

		if (n == 0) {
			result.add(new ArrayList<T>());
			return result;
		}

		List<List<T>> combinations = findSortedCombinations(elements, n - 1);
		for (List<T> combination: combinations) {
			for (T element: elements) {
				if (combination.contains(element)) {
					continue;
				}

				List<T> list = new ArrayList<T>();
				list.addAll(combination);

				if (list.contains(element)) {
					continue;
				}

				list.add(element);
				//sort items not to duplicate the items
				//   example: (a, b, c) and (a, c, b) might become  
				//   different items to be counted if not sorted   
				Collections.sort(list);

				if (result.contains(list)) {
					continue;
				}

				result.add(list);
			}
		}

		return result;
	}

	/**
	 * Basic Test of findSortedCombinations()
	 * 
	 * @param args 
	 */
	public static void main(String[] args) {
		List<String> elements = Arrays.asList("a", "b", "c", "d", "e");
		List<List<String>> combinations = findSortedCombinations(elements, 2);
		System.out.println(combinations);
	}

	/*
	 * sorts the map by values. Taken from:
	 * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
	 */

	@SuppressWarnings("rawtypes")
	public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {


			@SuppressWarnings("unchecked")
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		//LinkedHashMap will keep the keys in the order they are inserted
		//which is currently sorted on natural ordering
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}

	/*
	 * The toList() function accepts a transaction (items separated by commas) and returns a list of items. 
	 */
	public static List<String> toList(String transaction) {
		String[] items = transaction.trim().split(",");
		List<String> list = new ArrayList<String>();
		for (String item : items) {
			list.add(item);
		}
		return list;
	}

	/*
	 * The removeOneItem() function removes a single item from a given list and returns a
	 * new list with that item removed (used for generating the lefthand side of the associationrules).
	 */
	public static List<String>  removeOneItem(List<String> list, int i) {
		if ( (list == null) || (list.isEmpty()) ) {
			return list;
		}

		if ( (i < 0) || (i > (list.size()-1)) ) {
			return list;
		}

		List<String> cloned = new ArrayList<String>(list);
		cloned.remove(i);
		return cloned;
	}
	
	//tuple comparator for TOP_5
	public static Comparator<Tuple2<String, Integer>> item_number_comparator = new Comparator<Tuple2<String,Integer>>() {

        public int compare(Tuple2<String,Integer> tupleA, Tuple2<String, Integer> tupleB) {
            return tupleB._2.compareTo(tupleA._2);
        }

    };

	//tuple comparator for TOTAL_PER_MONTH
	public static Comparator<Tuple2<String, Double>> item_month_comparator = new Comparator<Tuple2<String,Double>>() {

        public int compare(Tuple2<String,Double> tupleA, Tuple2<String, Double> tupleB) {
			StringTokenizer t1=new StringTokenizer(tupleA._1,"-");
			StringTokenizer t2=new StringTokenizer(tupleB._1,"-");
			t1.nextToken();
			t2.nextToken();
        	int v1=Integer.parseInt(t1.nextToken());
        	int v2=Integer.parseInt(t2.nextToken());
        	
        	Integer a=v1;
        	Integer b=v2;
            return a.compareTo(b);
        }

    };

}
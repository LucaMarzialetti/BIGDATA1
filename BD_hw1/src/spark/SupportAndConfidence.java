package spark;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple4;

public class SupportAndConfidence {

	private static String path_to_dataset;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		if (args.length < 2) {
		        System.err.println("Usage: SupportAndConfidence <input_dataset_txt> <output>");
		        System.exit(1);}
		
		path_to_dataset=args[0];
		path_to_output_dir=args[1];
		//ESERCIZIO 3
		support_confidence();
		
	}//end main

	// Load the data from the text file and return an RDD of billings
	public static JavaRDD<String> loadData() { 
		// create spark configuration and spark context
		conf = new SparkConf().setAppName("SupportAndConfidence");//.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		//sc.addJar("MBA.jar");
		JavaRDD<String> billings = sc.textFile(path_to_dataset);
		return billings;
	}

	//confidence and support: da rivedere con attenzione
	@SuppressWarnings("serial")
	public static List<List<Tuple4<List<String>,List<String>,Double,Double>>> support_confidence() {

		//carica il file con i billing
		JavaRDD<String> billings = loadData();

		//crea un accumulatore condiviso, contenente il numero totale di billing
		final Accumulator<Integer> total_number_of_billings=sc.accumulator(0);
		billings.foreach(new VoidFunction<String>(){ public void call(String line) {total_number_of_billings.add(1);}});
		billings.cache();
		final double driver_accumulator=total_number_of_billings.value();

		//genera tutte le coppie di item possibili dagli elementi dello scontrino nel formato ([item1,item2],1)
		//e le coppie ([item],1) per ogni item nello scontrino
		//si usa flatmap anzichè map poiche vogliamo un rdd di coppie 
		JavaPairRDD<List<String>,Integer> couples_and_items = 
				billings.flatMapToPair(
						new PairFlatMapFunction<String, List<String>,Integer>() {
							public Iterable<Tuple2<List<String>,Integer>> call(String billing) {
								//prende la stringa che rappresenta lo scontrino 
								//e ne crea una lista di item
								List<String> items_in_billing = Utils.toList(billing);
								//rimozione data, non serve
								items_in_billing.remove(0);
								//inizializzazione lista risultato
								List<Tuple2<List<String>,Integer>> result = new ArrayList<Tuple2<List<String>,Integer>>();
								//caso in cui lo scontrino contiene un solo elemento
								if(items_in_billing.size()==1){
									List<List<String>> just_one=Utils.findSortedCombinations(items_in_billing,1);
									result.add(new Tuple2<List<String>,Integer>(just_one.get(0), 1));
								}
								else{
									//per ogni item emetti la coppia ([item],1)
									//serve per contarne le occorrenze totali
									for(String item:items_in_billing){
										List<String> item_as_list=new ArrayList<String>();
										item_as_list.add(item);
										result.add(new Tuple2<List<String>,Integer>(item_as_list, 1));}
									//genera la lista di tutte le "coppie" di item possibili con gli item nella lista
									//una coppia è in realtà ancora una lista di due elementi [item1,item2]
									List<List<String>> combinations = Utils.findSortedCombinations(items_in_billing,2);
									//aggiunge al risultato le tuple2 (tupla di 2 elementi):
									//ogni tuple2 contiene una lista di 2 elementi generata sopra ed il valore 1
									//esempio <[item1,item2],1>
									for (List<String> comb_list : combinations) {
										if (comb_list.size() > 0) {result.add(new Tuple2<List<String>,Integer>(comb_list, 1));}
									}
								}
								//questa funzione ritorna una lista di tuple2 <[item1,item2],1> e <[item1],1>
								//la flatmap farà in modo che l'rdd sia composto dalle coppie
								//in ogni lista di ogni billing.
								//se usassimo map, nell'rdd avremmo solo una lista per ogni billing
								return result;
							}
						});
		couples_and_items.cache();
		//couples_and_items.saveAsTextFile(path_to_output_dir+"1-couples_and_items");

		//riduzione: per ogni coppia somma per valore (gli 1), in questo modo ottengo coppie 
		//([item1,item2], frequenza_cooccorrenze_item1_e_item2) e ([item], freq_item)
		//la riduzione avviene prendendo i risultati di tutti gli rdd
		JavaPairRDD<List<String>, Integer> couples_and_items_reduced =
				couples_and_items.reduceByKey(
						new Function2<Integer, Integer, Integer>() {
							public Integer call(Integer i1, Integer i2) {return i1 + i2;}});  
		couples_and_items_reduced.cache();
		//couples_and_items_reduced.saveAsTextFile(path_to_output_dir+"/2-couples_and_items_reduced");

		/*
		 *  step intermedio:
		 *  a) per ogni coppia ([item1, item2], couple_freq) si emettono
		 *  le coppie ([item1],([item1, item2],2)) e ([item2],([item1, item2],2))
		 *  in questo modo si associano ad un item tutte le coppie in cui compare con le relative
		 *  frequenze
		 *   
		 *  ad esempio
		 *  dato in input ([latte, pesce], 2), dove 2 indica la frequenza 
		 * 	si generano in output
		 *	([pesce],([latte, pesce],2)) 
		 *	([latte],([latte, pesce],2))
		 *	
		 *	b) per ogni coppia ([item],freq_item), emetto ([item],([item],freq_item)))
		 *
		 */ 
		JavaPairRDD<List<String>,Tuple2<List<String>,Integer>> items_couples_and_frequency = 
				couples_and_items_reduced.flatMapToPair(
						new PairFlatMapFunction< Tuple2<List<String>,Integer>,  List<String>,  Tuple2<List<String>,Integer> >() {
							public Iterable< Tuple2<List<String>,Tuple2<List<String>,Integer>> > call(Tuple2<List<String>, Integer> couple_with_frequency) {
								//inizializza lista risultato da ritornare
								List<Tuple2<List<String>,Tuple2<List<String>,Integer>>> result = new ArrayList<Tuple2<List<String>,Tuple2<List<String>,Integer>>>();
								//elemento 1 della tuple2 in input=coppia [item1,item2]
								List<String> couple = couple_with_frequency._1;
								//elemento 2 della tuple2 in input= frequenza della coppia
								Integer frequency = couple_with_frequency._2;
								//aggiungi la coppia con la relativa frequenza alla lista delle tuple2 risultato
								//result.add(new Tuple2<List<String>, Tuple2<List<String>, Integer>>(couple, new Tuple2<List<String>, Integer>(null,frequency)));

								//se elemento 1 ha la forma [item1], è un contatore per quell' elemento->aggiungilo inalterato
								if (couple.size() == 1){
									result.add(new Tuple2<List<String>, Tuple2<List<String>, Integer>>(couple, new Tuple2<List<String>, Integer>(couple, frequency)));
									return result;}
								// altrimenti siamo nel caso [item1,item2])
								for (int i=0; i < couple.size(); i++) {
									List<String> one_item_in_couple = Utils.removeOneItem(couple, i);
									result.add(new Tuple2<List<String>, Tuple2<List<String>, Integer>>(one_item_in_couple, new Tuple2<List<String>, Integer>(couple, frequency)));
								}
								return result;
							}
						});
		items_couples_and_frequency.cache();
		//items_couples_and_frequency.saveAsTextFile(path_to_output_dir+"/3-item_couples_and_frequency");

		// combina item, coppie in cui appare, frequenza, in base alla chiave
		//         ([item2]			,[ 		([item2],item2_freq), ([item1,item2],couple_freq),([item3,item4],couple_freq1),([item5,item6],couple_freq2)  	] 	)
		//         ([item1]			,[		([item1],item1_freq), ([item1,item2],couple_freq),([item8,item9],couple_freq3),([item10,item11],couple_freq4)	]	)
		JavaPairRDD<List<String>,Iterable<Tuple2<List<String>,Integer>>> items_couples_and_freq_grouped = items_couples_and_frequency.groupByKey();       
		items_couples_and_freq_grouped.cache();
		//items_couples_and_freq_grouped.saveAsTextFile(path_to_output_dir+"/4-item_couples_and_freq_grouped");

		// calcolo supporto e confidenza per le regole di associazione item1->item2
		// output=T4(List(item1), List(item2), confidence, support)      
		JavaRDD<List<Tuple4<List<String>,List<String>,Double, Double>>> association_rules = 
				items_couples_and_freq_grouped.map(
						new Function< Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>>, List<Tuple4<List<String>,List<String>,Double, Double>> >() {
							public List<Tuple4<List<String>,List<String>,Double,Double>> call(Tuple2<List<String>,Iterable<Tuple2<List<String>,Integer>>> in) {
								//inizializza lista risultato
								List<Tuple4<List<String>,List<String>,Double,Double>> output = 	new ArrayList<Tuple4<List<String>,List<String>,Double,Double>>();

								//elemento1 della tupla2 input : singolo [item1]
								List<String> item1 = in._1;

								//elemento2 della tupla2 input = 
								//lista di coppie in cui item1 compare= [ ([item1,item2],n),([item3,item1],n1),([item1,item6],n2) ], se la chiave è [item1],
								//contiene anche coppie ([item1],frequenza_item)
								Iterable<Tuple2<List<String>,Integer>> item1_or_couple_valuelist = in._2;

								//inizializzazione lista di possibili item2 della regola
								List<Tuple2<List<String>,Integer>> item2_couple_list = new ArrayList<Tuple2<List<String>,Integer>>();
								double item1_counter=0;

								//per ogni tupla nella lista di coppie associata a item1_or_couple
								for (Tuple2<List<String>,Integer> tuple2 : item1_or_couple_valuelist) {
									// se il primo elemento ha dimensione 1, allora contiene la frequenza di quell' elemento 
									if (tuple2._1.size()==1) {item1_counter = tuple2._2;}
									//altrimenti inserisci le coppie nella lista di possibili item2 per la regola di associazione per item1_or_couple
									else {item2_couple_list.add(tuple2);}
								}
								if (item2_couple_list.isEmpty()) {return output;} 

								// creazione regole di associazione e calcolo supporto e confidenza:
								// per ogni tupla2 nella lista di coppie ((item, item2),cooccurrence))
								for (Tuple2<List<String>,Integer>  couple_cooccurrence : item2_couple_list) {
									double s = (double) couple_cooccurrence._2 / driver_accumulator;
									double c = (double) couple_cooccurrence._2 / (double) item1_counter;
									//[item1,item2]
									List<String> item2_as_list = new ArrayList<String>(couple_cooccurrence._1);
									//[item2]
									item2_as_list.removeAll(item1);
									output.add(new Tuple4<List<String>, List<String>, Double,Double>(item1, item2_as_list, s, c));
								}
								return output;
							}
						});   

		association_rules.saveAsTextFile(path_to_output_dir);
		List<List<Tuple4<List<String>,List<String>,Double,Double>>> result=association_rules.collect();
		//chiude il contesto
		sc.close();
		return result;    
	}//end support_and_confidence
}//end App
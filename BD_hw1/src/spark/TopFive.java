package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.Tuple4;

@SuppressWarnings("unused")
public class TopFive {

	private static String path_to_dataset;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		if (args.length < 2) {
		        System.err.println("Usage: TopFive <input_dataset_txt> <output>");
		        System.exit(1);
		}
		path_to_dataset=args[0];
		path_to_output_dir=args[1];
		//ESERCIZIO 1
		top5_per_month();
	}//end main

	// Load the data from the text file and return an RDD of billings
	public static JavaRDD<String> loadData() { 
		// create spark configuration and spark context
		conf = new SparkConf().setAppName("TopFive");//.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		//sc.addJar("MBA.jar");
		JavaRDD<String> billings = sc.textFile(path_to_dataset);
		return billings;
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	//Top 5 products per month
	@SuppressWarnings("serial")
	public static List<Tuple2<String,Iterable<Tuple2<String, Integer>>>> top5_per_month() {

		JavaRDD<String> billings = loadData();

		// mappa il formato in <<DATA,ITEM>,1>                                                       		input   output:K               output:V
		JavaPairRDD<Tuple2<String,String>,Integer> date_item_couples = 
				billings.flatMapToPair(
						new PairFlatMapFunction<String, Tuple2<String,String>, Integer>() {
							public Iterable<Tuple2< Tuple2<String,String>, Integer >> call(String billing) {
								if ((billing == null) || (billing.length() == 0)) {return Collections.emptyList();}           

								List<String> billing_items = new ArrayList<String>(Arrays.asList(billing.split(",")));
								StringTokenizer tokenizer=new StringTokenizer(billing_items.remove(0),"-");
								String date=tokenizer.nextToken()+"-"+tokenizer.nextToken();
								List<Tuple2<Tuple2<String, String>, Integer>> list = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
								for (String  item : billing_items) {
									list.add(new Tuple2< Tuple2<String,String>, Integer >(new Tuple2<String,String>(date, item), 1));
								}
								return list;
							}
						});

		//reduce in <<DATA,ITEM>,COUNT_PER_ITEM>  
		JavaPairRDD<Tuple2<String,String>, Integer> date_item_couples_reduced = 
				date_item_couples.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer t1, Integer t2) {return t1+t2;}
				});


		//cambia formato da <<DATA,ITEM>,COUNT_PER_ITEM> a <DATA, <ITEM,COUNT_PER_ITEM>> 
		JavaPairRDD<String,Tuple2<String,Integer>> date_as_key =
				date_item_couples_reduced.mapToPair(new PairFunction< Tuple2< Tuple2<String,String>,Integer>, String, Tuple2<String,Integer>>() {
					public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> arg0)	throws Exception {
						return new Tuple2<String, Tuple2<String, Integer>>( arg0._1._1, new Tuple2<String,Integer>(arg0._1._2,arg0._2));
					}
				});

		//raggruppa per chiave in <DATA, LIST[<ITEM,COUNT_PER_ITEM>]>
		JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> data_as_key_grouped = date_as_key.groupByKey();
		data_as_key_grouped.cache();

		//ordinamento item
		JavaPairRDD<String,Iterable<Tuple2<String,Integer>>> ordered_items =
				data_as_key_grouped.mapValues(
						new Function<Iterable<Tuple2<String,Integer>>,Iterable<Tuple2<String,Integer>>>() {
							public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> arg0)throws Exception {
								List<Tuple2<String, Integer>> sortedList = new ArrayList<Tuple2<String, Integer>>();
								int cont=0;
								for (Tuple2<String, Integer> t : arg0) {
									if(cont==5)
										break;
									sortedList.add(t);
									cont=cont+1;
								}
								Collections.sort(sortedList,Utils.item_number_comparator);
								return sortedList;
							}
						});

		ordered_items.cache();
		ordered_items.saveAsTextFile(path_to_output_dir);
		List<Tuple2<String,Iterable<Tuple2<String, Integer>>>> result=ordered_items.collect();
		//chiude il contesto
		sc.close();
		return result;
	}
}//end App
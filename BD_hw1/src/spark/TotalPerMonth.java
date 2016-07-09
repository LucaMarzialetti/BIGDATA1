package spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class TotalPerMonth {

	private static String path_to_dataset;
	private static String path_to_output_dir;
	private static SparkConf conf;
	private static JavaSparkContext sc;

	public static void main(String[] args) {
		if (args.length < 2) {
		        System.err.println("Usage: TotalPerMonth <input_dataset_txt> <output>");
		        System.exit(1);}
		path_to_dataset=args[0];
		path_to_output_dir=args[1];
		//ESERCIZIO 2
		total_per_item_per_month();
	}//end main

	// Load the data from the text file and return an RDD of billings
	public static JavaRDD<String> loadData() { 
		// create spark configuration and spark context
		conf = new SparkConf().setAppName("TotalPerMonth");//.setMaster("local[*]");
		sc = new JavaSparkContext(conf);
		//sc.addJar("MBA.jar");
		JavaRDD<String> billings = sc.textFile(path_to_dataset);
		return billings;
	}

	//Total per item per month
	@SuppressWarnings("serial")
	public static List<Tuple2<String,Iterable<Tuple2<String,Double>>>> total_per_item_per_month() {

		//formato originale: data[,item item_value]*  
		//per esempio 2015-01-03,pane 15,uova 12
		JavaRDD<String> billings_with_cost = loadData();

		//mappa il formato in <<ITEM,DATE>,VALUE_PER_ITEM_IN_BILL>                                                       			 input   output:K               output:V
		JavaPairRDD<Tuple2<String,String>,Double> couples = 
				billings_with_cost.flatMapToPair(
						new PairFlatMapFunction<String, Tuple2<String,String>, Double>() {
							public Iterable<Tuple2< Tuple2<String,String>, Double >> call(String billing) {
								if ((billing == null) || (billing.length() == 0)) {	return Collections.emptyList();}           
								//splitta lo scontrino attorno alle virgole
								List<String> billing_items = new ArrayList<String>(Arrays.asList(billing.split(",")));
								//estrazione della data (è sempre il primo campo)
								StringTokenizer tokenizer=new StringTokenizer(billing_items.remove(0),"-");
								String date=tokenizer.nextToken()+"-"+tokenizer.nextToken();
								Double item_value;
								String item_name;
								List<Tuple2<Tuple2<String, String>, Double>> result = new ArrayList<Tuple2<Tuple2<String, String>, Double>>();
								
								//per ogni item nello scontrino emetti una coppia nel formato indicato
								for (String  item : billing_items) {
									StringTokenizer tokenizer2=new StringTokenizer(item," ");
									item_value=Double.valueOf(tokenizer2.nextToken());
									item_name=tokenizer2.nextToken();
									result.add(new Tuple2< Tuple2<String,String>, Double >(new Tuple2<String,String>(item_name,date), item_value));
								}
								return result;
							}
						});

		//couples.saveAsTextFile(path_to_output_dir+"/couples");

		//reduce in <<ITEM,DATA>,TOT_VALUE_PER_ITEM>  
		JavaPairRDD<Tuple2<String,String>, Double> couples_reduced = 
				couples.reduceByKey(new Function2<Double, Double, Double>() {
					public Double call(Double t1, Double t2) {return t1+t2;}
				});

		//couples_reduced.saveAsTextFile(path_to_output_dir+"/couples_reduced");

		//cambia formato da <<ITEM,DATA>,TOT_VALUE_PER_ITEM> a <ITEM, <DATA,TOT_VALUE_PER_ITEM>> 
		JavaPairRDD<String,Tuple2<String,Double>> item_as_key =
				couples_reduced.mapToPair(new PairFunction< Tuple2< Tuple2<String,String>,Double>, String, Tuple2<String,Double>>() {
					public Tuple2<String, Tuple2<String, Double>> call(Tuple2<Tuple2<String, String>, Double> arg0)	throws Exception {
						return new Tuple2<String, Tuple2<String, Double>>( arg0._1._1, new Tuple2<String,Double>(arg0._1._2,arg0._2));
					}
				});

		//item_as_key.saveAsTextFile(path_to_output_dir+"/item_as_key");

		//raggruppa: output = <item, lista[<mese,tot_mese>]>
		JavaPairRDD<String,Iterable<Tuple2<String,Double>>> item_grouped = item_as_key.groupByKey();
		//item_grouped.saveAsTextFile(path_to_output_dir+"/result");

		//ordinamento mesi
		JavaPairRDD<String,Iterable<Tuple2<String,Double>>> ordered_month =
				item_grouped.mapValues(
						new Function<Iterable<Tuple2<String,Double>>,Iterable<Tuple2<String,Double>>>() {
							public Iterable<Tuple2<String, Double>> call(Iterable<Tuple2<String, Double>> arg0)throws Exception {
								List<Tuple2<String, Double>> sortedList = new ArrayList<Tuple2<String, Double>>();
								for (Tuple2<String, Double> t : arg0) {sortedList.add(t);}
								Collections.sort(sortedList,Utils.item_month_comparator);
								return sortedList;
							}
						});


				
		ordered_month.cache();
		ordered_month.saveAsTextFile(path_to_output_dir);		
		List<Tuple2<String, Iterable<Tuple2<String, Double>>>> result= ordered_month.collect();
		sc.close();
		return result;
	}//end total_per_month
}//end App
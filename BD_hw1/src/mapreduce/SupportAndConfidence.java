package mapreduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SupportAndConfidence extends Configured implements Tool{

	public static enum COUNTER {
		TotalBillingsCounter;
	}
	
	public static void main (String[] args) throws Exception{
		
		int res=ToolRunner.run(new Configuration(),  new SupportAndConfidence(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: SupportAndConfidence <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf);
		job.setJobName("SupportAndConfidence");
		job.setJarByClass(SupportAndConfidence.class);
		job.setMapperClass(SupportAndConfidenceMapper.class);
		job.setReducerClass(SupportAndConfidenceReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * dato il formato di input, si creano tutte le permutazioni possibili degli item in un billing,
	 * emettendo poi coppie nel formato <ITEM_IN_BILL, [EVERY_OTHER_ITEM_IN_SAME_BILL]>.
	 * Per ogni riga(cioè billing) viene incrementato un contatore globale.
	 */
	public static class SupportAndConfidenceMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//esempio ["2015-03-02","pane","burro","latte"]
			String[] billing_items = value.toString().split(",");

			int i=1;
			int j=1;
			int length=billing_items.length;
			String temp="";

			//esempio ["pane:burro latte","burro:pane latte","latte:pane burro"]
			String[] billing_items_permutations = new String[length-1];

			//crea  tutte le combinazioni possibili di coppie di prodotti (in un billing)
			while(j<length){
				String app=billing_items[j];
				temp=app+":";
				while(i<length){
					if(!app.equals(billing_items[i])){
						if(i+1==length){
							temp=temp+billing_items[i];}
						else{
							temp=temp+billing_items[i]+" ";}
					}
					i=i+1;
				}
				billing_items_permutations[j-1]=temp;
				temp="";
				j=j+1;
				i=1;
			}

			System.out.println();

			String[] item_items_in_same_bill=new String[2];

			//per ogni item nel billing, scrive nel contesto coppie (item, prodotti_nello_stesso_billing_dell_item)
			//esempio da ["pane:burro latte","burro:pane latte","latte:pane burro"],
			//si scrive (Text(pane), Text(burro latte)),(Text(burro), Text(pane latte)),(Text(latte), Text(pane burro))
			for(String s:billing_items_permutations){
				item_items_in_same_bill=s.split(":");
				context.write(new Text(item_items_in_same_bill[0]), (item_items_in_same_bill.length > 1)? new Text(item_items_in_same_bill[1]) : new Text(item_items_in_same_bill[0]));				
			} 
			//incrementa il counter di 1 per ogni billing mappato
			context.getCounter(COUNTER.TotalBillingsCounter).increment(1);
		}
	}

	/**
	 * output dello shuffle/sort= coppie <ITEM1, [[ITEMS2_IN_SAME_BILL]]>, ovvero a ogni ITEM1 � associata una lista di billings, dove ogni billing �
	 * rappresentato a propria volta dalla lista di ITEMS2 che cooccorrono con ITEM1. Per ogni ITEM2 per ogni billing si calcola quante volte ogni ITEM2
	 * compare (cio� le cooccorrence con ITEM), usate poi per calcolare confidenza e supporto relative a ITEM.
	 * In output vengono emesse coppie <"ITEM1","->ITEM2:SUPPORT%,CONFIDENCE%">, una per ogni regola di associazione ITEM1->ITEM2.
	 */
	public static class SupportAndConfidenceReducer extends Reducer<Text, Text, Text, Text> {

		private long mapperCounter;

		//prende il contatore globale di scontrini dal contesto,per usarlo nel calcolo del supporto delle regole
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			Cluster cluster = new Cluster(conf);
			Job currentJob = cluster.getJob(context.getJobID());
			mapperCounter = currentJob.getCounters().findCounter(COUNTER.TotalBillingsCounter).getValue();  
		}

		//			key_item	value_billings
		//			            bill			   bill  		     bill         bill				=>somma=num. tot. di scontrini in cui item compare
		//							 item2 item2	   item2 item2       item2        item2 item2
		//esempio (Text(pane), [Text(burro latte), Text(uova latte), Text(latte), Text(uova acqua)])
		@Override
		public void reduce(Text key_item1, Iterable<Text> value_billings, Context context) throws IOException, InterruptedException {

			Map<String, Integer> associations = new HashMap<String, Integer>();

			int number_of_billings_of_item1=0;


			/* calcola le cooccorrenze di item con ogni item2 creando una mappa dalla lista value_billings associata all item
			 * il numero di volte che ogni item2 cooccorre con item consente di calcolare il supporto e la confidenza
			 * per la regola item->item2 
			 * 
			 * esempio:
			 * 
			 * burro->1
			 * latte->3      cio� latte compare in 3 scontrini in cui compare anche pane
			 * uova->2
			 * acqua->1
			 */
			for (Text bill : value_billings) {
				number_of_billings_of_item1=number_of_billings_of_item1+1;
				String[] items_in_bill=bill.toString().split(" ");
				for(String item2: items_in_bill){
					if (associations.containsKey(item2)) {
						// Map already contains item2. Just increment its billing count by 1
						int number_of_bills=associations.get(item2)+1;
						associations.put(item2, number_of_bills);

					} else {
						// Map doesn't have mapping for month. Add one with count = cost of item
						associations.put(item2, 1);
					}
				}
			}

			String res="";
			double cooccurrence;
			double support;
			double confidence;

			int total_number_of_billings=(int) mapperCounter;

			for (String item2 : associations.keySet()) {
				cooccurrence=associations.get(item2);

				/*
				 * per la regola item->item2: 
				 * 
				 * confidenza:
				 * -item e item2 compaiono nello stesso scontrino "cooccurrence"-volte
				 * -item compare in "number_of_billings_of_item1"-scontrini
				 * 
				 * supporto:
				 * -item e item2 compaiono nello stesso scontrino "cooccurrence"-volte
				 * -il numero totale di scontrini � preso dal contesto
				 * 
				 */
				support=(cooccurrence/total_number_of_billings)*100;
				confidence=(cooccurrence/number_of_billings_of_item1)*100;

				res="\t->\t"+item2+":"+new DecimalFormat("###.##").format(support)+"% | "+new DecimalFormat("###.##").format(confidence)+"%\t";
				Text result = new Text(res);

				context.write(key_item1, result);	
			}
		}
	}
}


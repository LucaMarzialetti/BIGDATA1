package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopFive extends Configured implements Tool {

	/** Main: eseguito in seugito alla chiamate della jar
	 *  Usa configuration e classe come parametri **/
	public static void main (String[] args) throws Exception{
		if(args.length != 2 )
			System.out.println("Usgage: Top5 <input file> <output file>");
		else {
			int res=ToolRunner.run(new Configuration(),  new TopFive(), args);
			System.exit(res);
		}
	}

	/** Configurazione del running del job **/
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf);
		job.setJobName("Top5");
		job.setJarByClass(TopFive.class);
		job.setMapperClass(TopFiveMapper.class);
		job.setReducerClass(TopFiveReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**Mapper:
	 *dato il formato in input, crea coppie <chiave,valore> nel formato <AAAA-MM,item>
	 *1 per ogni item in un billing
	 */
	public static class TopFiveMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			/*STRUTTURA{1°campo=data,altri=items}*/
			String[] parts = value.toString().split(",");
			/*formatto la data*/
			String data = parts[0];
			StringTokenizer tokenizer=new StringTokenizer(data,"-");
			data=tokenizer.nextToken()+"-"+tokenizer.nextToken();
			/*wrapping per la data*/
			Text year_and_month = new Text();
			year_and_month.set(data);
			/*emit*/
			for(int i=1; i<parts.length; i++)
				context.write(year_and_month, new Text(parts[i]));
		}
	}

	/** Reducer:
	 * lo shuffle/sort restituisce coppie <AAAA-MM,[lista di item venduti nel MM]>
	 * per ogni item si crea una mappa in cui l'item stesso è chiave, il valore è il numero di volte che compare nella lista. 
	 * La mappa viene ordinata in ordine decrescente in base a tale valore. 
	 * Si scrive dunque in output una coppia <"AAAA-MMM", 5*("ITEM:NUMERO_VOLTE_VENDUTO")>.
	 * Una per ognuno dei 5 prodotti piu venduti
	 */
	public static class TopFiveReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key_month, Iterable<Text> value_items, Context context) throws IOException, InterruptedException {
			/*mappa usata per emit del risultato. contiene data<AAAA-MM, 5*(items:count)>*/
			Map<String, Integer> month_items = new HashMap<String, Integer>();
			for (Text item : value_items) {
				//valore iniziale 1
				int val_to_put = 1;
				if (month_items.containsKey(item.toString())) {
					// se la mappa già lo contiene 1+valore nella mappa = è un incremento
					val_to_put+=month_items.get(item.toString());
				}
				// aggiungo alla mappa con valore corretto
				month_items.put(item.toString(), val_to_put);
			}
			/*sorting per valori*/
			Map<String, Integer> sortedMap = Utils.sortByValues(month_items);
			/*count dei primi 5*/
			int counter = 0;
			String res="";
			for (String item : sortedMap.keySet()) {
				if (counter > 4) {
					res+=item+":"+sortedMap.get(item);
					break;
				}
				res+=item+":"+sortedMap.get(item)+"\t";
				counter++;
			}
			/*wrapping per il risultato*/
			Text result = new Text(res);
			/*emit*/
			context.write(new Text(key_month.toString()+"\t"), result);
		}
	}
}
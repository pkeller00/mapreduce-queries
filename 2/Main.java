import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Main {


  /**
  * MapReduce - Mapper. Creates {key,value} pair which is then passed onto the Reducer. 
  *
  * @interface  Object database object. Data on which the queries are run. 
  * @interface  Text row from database file
  * @interface  Text ss_store_sk acts as key.
  * @interface  Text total ss_net_paid for given key.
  */
  public static class StoreSalesMapper extends Mapper<Object, Text, Text, Text>{

    /*
    HashMap which acts as a local combiner. Groups values for ss_net_paid by ss_store_sk to 
    minimise data passed to reducer
    */
    HashMap<Text, Double> netSalesStore = new HashMap<Text, Double>();

    /**
    * Mapper function. Retrieves relevant features; matches date range criteria; ignores NULL entries. 
    * Uses a hashmap to reduce amount of data that is being passed to the Reducer.
    *
    * @param key Object key.
    * @param value entry in the database.
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      
      String[] tokens;
      tokens = line.split("\\|",-1);//tokinising an entry spliting by '|'.

      // Ensuring none of the relevant features are null.
      final int sold_date = 0;
      final int store_sk = 7;
      final int net_paid = 20;
      boolean is_null = (tokens[sold_date].isEmpty()) || (tokens[store_sk].isEmpty()) || ( tokens[net_paid].isEmpty());
      
      // If all relevant features exist, check against predefined criteria.
      if(!is_null){

        // Creates variable for each token in the record.
        Integer ss_sold_date_sk = Integer.parseInt(tokens[sold_date]);
        Text ss_store_sk = new Text(tokens[store_sk]);
        Double ss_net_paid = Double.parseDouble(tokens[net_paid]);

        Configuration conf = context.getConfiguration();
        Integer start_date = Integer.parseInt(conf.get("start_date"));
        Integer end_date = Integer.parseInt(conf.get("end_date"));
        
        if((ss_sold_date_sk >= start_date) && (ss_sold_date_sk <= end_date)){

          // If key in the HashTable does not exist add as new.
          if(netSalesStore.get(ss_store_sk) == null){
            netSalesStore.put(ss_store_sk, ss_net_paid);
          }
          // Otherwise, add to the value of existing key.
          else{
            netSalesStore.put(ss_store_sk,  netSalesStore.get(ss_store_sk) + ss_net_paid);
          }
        }
      }  
    }

    /**
    * Mapper's cleanup function. Context writes values from the HashMap. 
    *
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Text key : netSalesStore.keySet()) {
        //adds a 'net' prefix to know these key,value pairs are for net_paid.
          context.write(key,new Text("net\t" + String.valueOf(netSalesStore.get(key))));
      } 
    }

  }


  /**
  * MapReduce - Mapper. Creates {key,value} pair which is then passed onto the Reducer. 
  *
  * @interface  Object database object. Data on which the queries are run. 
  * @interface  Text row from database file
  * @interface  Text ss_store_sk acts as key.
  * @interface  Text total s_floor_sapce for given key.
  */
  public static class StoresMapper extends Mapper<Object, Text, Text, Text>{

    /*
    HashMap which acts as a local combiner. Groups values for s_floor_sapce by ss_store_sk to 
    minimise data passed to reducer
    */
    HashMap<Text, Integer> totalNetTax = new HashMap<Text, Integer>();

    /**
    * Mapper function. Retrieves relevant features; matches date range criteria; ignores NULL entries. 
    * Uses a hashmap to reduce amount of data that is being passed to the Reducer.
    *
    * @param key Object key.
    * @param value entry in the database.
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      
      String[] tokens;
      tokens = line.split("\\|",-1);//tokinising an entry spliting by '|'.

      // Ensuring none of the relevant features are null.
      final int sold_date = 0;
      final int floor_space = 7;
      boolean is_null = (tokens[sold_date].isEmpty()) || ( tokens[floor_space].isEmpty());
      if(!is_null){

        Text ss_store_sk = new Text(tokens[sold_date]);
        Integer s_floor_space = Integer.parseInt(tokens[floor_space]);

        totalNetTax.put(ss_store_sk, s_floor_space);
      }  
    }

    /**
    * Mapper's cleanup function. Context writes values from the HashMap. 
    *
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Text key : totalNetTax.keySet()) {
          context.write(key,new Text("flr\t" + String.valueOf(totalNetTax.get(key))));
      } 
    }

  }

  /**
  * Reducer function. Calculates how much total net paid we have per store.
  *
  * @interface  Text ss_store_sk acts as key.
  * @interface  Text total ss_net_paid_tax for given key and its floor space.
  * @interface  Text ss_store_sk acts as key.
  * @interface  Text total ss_net_paid_tax for given key and its floor space.
  */
  public static class JoinReducer extends Reducer<Text,Text,Text,Text> {
    
    //Stores all stores in a red-black tree. Only keeps the top k stores removing all other nodes.
    //Uses a custom data type to allow for easy sorting
    private TreeMap<StoreInfo, Text> TopKMap = new TreeMap<StoreInfo, Text>() ;

    /**
     * Rounds a double to a specified number of places.
     * @param value number to round.
     * @param places number of places to round to.
     * @return rounded number.
     */
    public double round(double value, int places){

      BigDecimal bd = BigDecimal.valueOf(value);
      bd = bd.setScale(places, RoundingMode.HALF_UP);
      
  
      return bd.doubleValue();
    }

    /**
     * Function to sum all ss_net_paid and get its floor space per store.
     * 
     * @param key ss_store_sk acts as key.
     * @param values total ss_net_paid_tax for a given key along with its floor space.
     * @param context MapReduce next pipeline stage.
     */
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      Integer K = Integer.parseInt(conf.get("K"));//get how many stores user wants

      double net_total = 0;
      int floor = 0;

      //sum net paids for given store and get its floor space
      for (Text val : values) {
        String[] parts;
        parts = val.toString().split("\\t",-1);

        if (parts[0].equals("net")){
          net_total += Double.parseDouble(parts[1]);
        }
        else if (parts[0].equals("flr")) {
          floor = Integer.parseInt(parts[1]);
        }

      }

      //If we have more than K elements in tree than remove smallest one as we don't need it
      //this ensures tree doesn't store unecessary data and thus get too big.
      TopKMap.put(new StoreInfo((double)floor,net_total),new Text(key.toString()));
      if (TopKMap.size() > K) {
        TopKMap.remove(TopKMap.firstKey());
      }
            
    }
    
    /**
    * Reducer's cleanup function. Context writes values from the tree. 
    *
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (StoreInfo key : TopKMap.descendingKeySet()) {
          String key_str = Integer.toString((int)key.floor_space) + "\t" +Double.toString(key.net_paid);
          context.write(TopKMap.get(key),new Text(key_str));
      }   
    }
  
  }


  /**
   * Sets config for mapreduce job
   * @param args input args to the file
   * @return None
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    conf.set("K", args[0]);
    conf.set("start_date", args[1]);
    conf.set("end_date", args[2]);

    Job job = Job.getInstance(conf, "joinFloorNetPaid");

    job.setJarByClass(Main.class);
    job.setReducerClass(JoinReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, StoreSalesMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[4]), TextInputFormat.class, StoresMapper.class);

    FileOutputFormat.setOutputPath(job, new Path(args[5]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}


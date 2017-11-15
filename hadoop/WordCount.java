import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
    String  rem = itr.nextToken().toString();
    rem = rem.replaceAll("[^a-zA-Z]","");
    if(rem.length()!=0){
	Integer len  = new Integer(rem.length());
	StringTokenizer cov = new StringTokenizer(len.toString());
	word.set(cov.nextToken());
        context.write(word, one);
      }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
    if(!job.waitForCompletion(true)){
    	System.exit(1);
    }


    FileSystem fs = FileSystem.get(new Configuration());
    FileStatus[] status =  fs.listStatus(new Path(args[1]));
    ArrayList<hist> al = new ArrayList<hist>();
    for (int i = 1;i<status.length;i++){
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
    try{
        //StringBuilder sb = new StringBuilder();
        String line = null;
        String[] splited;
        while ((line = br.readLine())!=null){
                splited = line.split("\\s+");
                al.add(new hist(splited[0], splited[1]));
        }
    } catch(Exception e) {
         e.printStackTrace();
    }
    }
    Collections.sort(al, new Comparator<hist>(){
        @Override public int compare(hist p1, hist p2){
                return (Integer.compare(Integer.parseInt(p1.length),Integer.parseInt(p2.length)));
        }
    });
    System.out.println();
    int maxlen = 0;
    int tpcount = 0;
    int plen = 0;
    for(hist h: al){
        if(maxlen < h.count)
            maxlen = h.count;
        if(Integer.parseInt(h.length)>=20){
            tpcount+=h.count;
        }
    }
    plen = Integer.toString(maxlen).length();
    for(hist h : al){
        String output;
        int starnum = 0;
        float s;
        if(Integer.parseInt(h.length)<20){

            if(h.length.length() == 2){
                output = " "+h.length;
            }else{
                output = "  "+h.length;
            }
            output+=": ";
            s = (float)h.count/(float)maxlen;
            starnum = (int)(s*40.0);
            for(int i = 0; i<starnum; i++)
                output+="*";
            for(int i=0;i<plen-Integer.toString(h.count).length();i++)
                output+=" ";
            for(int i=0;i<40-starnum;i++)
                output+=" ";
            output+="  ";
            output+=Integer.toString(h.count);
            System.out.println(output);
        }
        else{
            output="20+: ";
            s = (float)tpcount/(float)maxlen;
            starnum = (int)(s*15.0);
            for(int i = 0; i<starnum; i++)
                output+="*";
            for(int i=0;i<plen-Integer.toString(h.count).length();i++)
                output+=" ";
            for(int i=0;i<40-starnum;i++)
                output+=" ";
            output+=Integer.toString(h.count);
            System.out.println();
            System.out.println(output);
            break;            
        }
    }
  }

 public static class hist{
        String length;
        int count;
        public hist(String l, String c){
                length = l;
                count = Integer.parseInt(c);
        }
  }

}

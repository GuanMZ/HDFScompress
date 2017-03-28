import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by MuzhiGuan on 2017/3/13.
 */
public class HDFScompress {

    public static class UserTimeMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,"\n");
            String temp = null;

            while (tokenizer.hasMoreTokens()) {
                try{
                    temp = tokenizer.nextToken();
                }catch(NumberFormatException e){continue;}
                context.write(new Text(temp), new Text(""));
            }
        }
    }
    public static class UserTimeReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args) throws Exception
    {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf1 = new Configuration();
        conf1.set("mapreduce.reduce.memory.mb","8192");
        Job job1 = new Job(conf1, "IDTimeSort");
        job1.setJarByClass(HDFScompress.class);
        job1.setMapperClass(UserTimeMapper.class);
        job1.setReducerClass(UserTimeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
//        job1.setNumReduceTasks(10);

        FileInputFormat.addInputPath(job1, inputPath);//
        FileOutputFormat.setOutputPath(job1, outputPath);//
        FileOutputFormat.setCompressOutput(job1, true);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}

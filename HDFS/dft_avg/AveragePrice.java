import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class AveragePrice {

    //Driver Class
    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job job = new Job(c, "average price");
        job.setJarByClass(AveragePrice.class);
        job.setMapperClass(MapForAverage.class);
        job.setReducerClass(ReduceForAverage.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        //get input paths from aruguments
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
        System.exit(0);
    }


    //Mapper
    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FloatWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.contains("id") == false)
            {
                String[] words = line.split(",");
                Text outputKey = new Text("Average price");
                FloatWritable outputValue = new FloatWritable(Float.parseFloat(words[7].trim()));
                context.write(outputKey, outputValue);
            }
        }
    }

    //Reducer
    public static class ReduceForAverage extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text word, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;
            for (FloatWritable value : values) {
                sum += value.get();
                count++;
                
            }

            float average = sum / count;
            context.write(word, new FloatWritable(average));
        }
    }
}
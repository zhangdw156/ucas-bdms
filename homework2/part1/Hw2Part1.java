import java.io.IOException;
import java.text.DecimalFormat;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hw2Part1 {

    public static class SourceDestinationMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text sourceDestKey = new Text();
        private Text countAndTime = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] parts = line.split("\\s+");

            if (parts.length != 3) {
                return;
            }

            try {
                String source = parts[0];
                String destination = parts[1];
                double time = Double.parseDouble(parts[2]);

                sourceDestKey.set(source + " " + destination);

                countAndTime.set(1+" "+time);

                context.write(sourceDestKey, countAndTime);
            } catch (NumberFormatException e) {
                return;
            }
        }
    }

    public static class SourceDestinationCombiner
            extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sumCount = 0;
            double sumTime = 0.0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                sumCount += Integer.valueOf(parts[0]);
                sumTime += Double.valueOf(parts[1]);
            }

            result.set(sumCount + " " + sumTime);
            context.write(key, result);
        }
    }

    public static class SourceDestinationReducer
            extends Reducer<Text, Text, Text, Text> {

        private Text resultValue = new Text();
        private DecimalFormat df = new DecimalFormat("0.000");

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int totalCount = 0;
            double totalTime = 0.0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\s+");
                totalCount += Integer.valueOf(parts[0]);
                totalTime += Double.valueOf(parts[1]);
            }

            double averageTime = totalTime / totalCount;
            String formattedAvgTime = df.format(averageTime);

            resultValue.set(totalCount + " " + formattedAvgTime);

            context.write(key, resultValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: SourceDestinationAnalysis <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "source destination analysis");
        job.setJarByClass(Hw2Part1.class);

        job.setMapperClass(SourceDestinationMapper.class);
        job.setCombinerClass(SourceDestinationCombiner.class);
        job.setReducerClass(SourceDestinationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
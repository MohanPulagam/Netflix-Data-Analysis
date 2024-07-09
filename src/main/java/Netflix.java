import java.io.*;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Netflix {

    public static class MyMapperJobOne extends Mapper<Object,Text,IntWritable,IntWritable> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
           String str=value.toString();
           boolean checksuffix=str.endsWith(":");
           if(checksuffix==false)
           {
                String tokenval[]=str.split(",");
                int firstval=Integer.parseInt(tokenval[0]);
                int secondval=Integer.parseInt(tokenval[1]);
                context.write(new IntWritable(firstval),new IntWritable(secondval));
           }

        }
    }
        
        public static class MyReducerJobOne extends Reducer<IntWritable,IntWritable,IntWritable,DoubleWritable> {
            @Override
            public void reduce ( IntWritable key, Iterable<IntWritable> values, Context context )
                               throws IOException, InterruptedException {
                double totalsum=0;
                int count=0;
                for (IntWritable v: values) {
                    totalsum +=v.get();
                    count++;
                };
                double finalsum=Math.floor(totalsum/count*10);
                context.write(key,new DoubleWritable(finalsum));
            }
        }
        

       public static class MyMapperJobTwo extends Mapper<Object,Text,DoubleWritable,IntWritable> {
            @Override
            public void map ( Object key, Text value, Context context )
                            throws IOException, InterruptedException {
               String str=value.toString();
               String tokenval[]=str.split("\t");
               int firstval=Integer.parseInt(tokenval[0]);
               double secondval=Double.parseDouble(tokenval[1]);
               context.write(new DoubleWritable(secondval),new IntWritable(1));
            }
       }
            
            public static class MyReducerJobTwo extends Reducer<DoubleWritable,IntWritable,DoubleWritable,IntWritable> {
                @Override
                public void reduce ( DoubleWritable key, Iterable<IntWritable> values, Context context )
                                   throws IOException, InterruptedException {
                    int count=0;
                    for (IntWritable v: values) {
                        count++;
                    };
                    double finalkey=key.get()/10.0;
                    context.write(new DoubleWritable(finalkey),new IntWritable(count));
                }
            }
    


    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("MyJobOne");
        job.setJarByClass(Netflix.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(MyMapperJobOne.class);
        job.setReducerClass(MyReducerJobOne.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        job.waitForCompletion(true);
        

        Job jobone = Job.getInstance();
        jobone.setJobName("MyJobTwo");
        jobone.setJarByClass(Netflix.class);
        jobone.setOutputKeyClass(DoubleWritable.class);
        jobone.setOutputValueClass(IntWritable.class);
        jobone.setMapOutputKeyClass(DoubleWritable.class);
        jobone.setMapOutputValueClass(IntWritable.class);
        jobone.setMapperClass(MyMapperJobTwo.class);
        jobone.setReducerClass(MyReducerJobTwo.class);
        jobone.setInputFormatClass(TextInputFormat.class);
        jobone.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(jobone,new Path(args[1]));
        FileOutputFormat.setOutputPath(jobone,new Path(args[2]));
        jobone.waitForCompletion(true);
        
    }
}


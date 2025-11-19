import org.apache.hadoop.conf.*; 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.*; 
import org.apache.hadoop.mapreduce.lib.output.*; 

public class WMR {

    public static class WeatherMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object k, Text v, Context c) throws java.io.IOException, InterruptedException {
            String[] f = v.toString().split(",");
            double t = Double.parseDouble(f[1]);
            String w;
            if (t < 0) w = "Freezing";
            else if (t <= 15) w = "Cold";
            else if (t <= 25) w = "Warm";
            else w = "Hot";
            c.write(new Text(f[0]), new Text(w));
        }
    }

    public static class WeatherReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text k, Iterable<Text> v, Context c) throws java.io.IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text x : v) sb.append(x).append(" ");
            c.write(k, new Text(sb.toString().trim()));
        }
    }

    public static void main(String[] a) throws Exception {
        Job j = Job.getInstance(new Configuration(), "Weather");
        j.setJarByClass(WMR.class);
        j.setMapperClass(WeatherMapper.class);
        j.setCombinerClass(WeatherReducer.class);
        j.setReducerClass(WeatherReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(a[0]));
        FileOutputFormat.setOutputPath(j, new Path(a[1]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}

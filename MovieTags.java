import org.apache.hadoop.conf.*; 
import org.apache.hadoop.fs.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*; 
import org.apache.hadoop.mapreduce.lib.input.*; 
import org.apache.hadoop.mapreduce.lib.output.*; 
import java.io.*; 
import java.util.*;

public class MovieTags {

    public static class TagMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object k, Text v, Context c) throws IOException, InterruptedException {
            String[] t = v.toString().split(",");
            if (t.length == 4) c.write(new Text(t[1]), new Text(t[2]));
        }
    }

    public static class TagReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text k, Iterable<Text> vals, Context c) throws IOException, InterruptedException {
            Set<String> s = new HashSet<>();
            for (Text x : vals) s.add(x.toString());
            c.write(k, new Text(String.join(", ", s)));
        }
    }

    public static void main(String[] a) throws Exception {
        Job j = Job.getInstance(new Configuration(), "MovieTags");
        if (a.length != 2) System.exit(1);
        j.setJarByClass(MovieTags.class);
        j.setMapperClass(TagMapper.class);
        j.setReducerClass(TagReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j, new Path(a[0]));
        FileOutputFormat.setOutputPath(j, new Path(a[1]));
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }
}

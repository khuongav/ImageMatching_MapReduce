import com.sun.org.apache.xml.internal.security.exceptions.Base64DecodingException;
import com.sun.org.apache.xml.internal.security.utils.Base64;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

public class Main {
  public static final String DEFAULT_FILE_SYSTEM = "fs.defaultFS";
  public static final String URI_FILE_SYSTEM = "hdfs://master:9000";

  public static class SearchMapper extends Mapper<Text, BytesWritable, NullWritable, SumNames> {
    private byte[] searchByte = new byte[64];

    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      try {
        this.searchByte = Base64.decode(conf.get("searchString"));
      } catch (Base64DecodingException e) {
        e.printStackTrace();
      }
    }

    public void map(Text key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
      byte[] bytes = value.copyBytes();
      byte[] result = new byte[64];

      Skein512.hash(bytes, result);
      if (Arrays.equals(this.searchByte, result)) {
        context.write(NullWritable.get(), new SumNames(1, key.toString()));
      }
    }
  }

  public static class IntSumReducer
      extends Reducer<NullWritable, SumNames, NullWritable, SumNames> {
    public void reduce(NullWritable key, Iterable<SumNames> values, Context context)
        throws IOException, InterruptedException {
      SumNames totalSumNames = new SumNames();
      for (SumNames val : values) {
        totalSumNames.addsumNames(val);
      }
      context.write(NullWritable.get(), totalSumNames);
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    // args[0]: path of the image to search

    double time_t = System.currentTimeMillis();

    byte[] searchByte = Main.generateImageHash(args[0]);

    String searchString = Base64.encode(searchByte);
    Configuration conf = new Configuration();
    conf.set(DEFAULT_FILE_SYSTEM, URI_FILE_SYSTEM);
    conf.set("searchString", searchString);

    conf.set("yarn.nodemanager.resource.memory-mb", "7168");
    conf.set("yarn.scheduler.minimum-allocation-mb", "256");
    conf.set("mapreduce.map.memory.mb", "3072");
    conf.set("mapreduce.reduce.memory.mb", "256");

    conf.set("mapreduce.input.fileinputformat.split.maxsize", "3221225472");
    conf.set("mapreduce.input.fileinputformat.split.minsize", "3221225472");

    // input is a sequence file in below path
    Path inputPath = new Path("/images/images_seq");
    Path outputPath = new Path("/images/result");
    // HdfsUtil.removeExistDir(outputPath.toString());

    Job job = Job.getInstance(conf, "Image Matching");
    job.setJarByClass(Main.class);
    job.setMapperClass(SearchMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(SumNames.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.setNumReduceTasks(1);

    job.waitForCompletion(true);
    System.out.print("\n Time taken: ");
    System.out.println((double) System.currentTimeMillis() - time_t);
  }

  private static byte[] readData(String fileName) throws IOException {
    File file = new File(fileName);
    byte[] fileData = new byte[(int) file.length()];
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    dis.readFully(fileData);
    dis.close();
    return fileData;
  }

  private static byte[] generateImageHash(String filePath) throws IOException {
    byte[] data = Main.readData(filePath);
    byte[] result = new byte[64];
    Skein512.hash((byte[]) data, (byte[]) result);
    return result;
  }
}

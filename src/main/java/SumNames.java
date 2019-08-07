import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumNames implements WritableComparable<SumNames> {
  IntWritable sum;
  Text names;

  public SumNames() {
    this.set(new IntWritable(0), new Text(""));
  }

  public SumNames(Integer sum, String names) {
    this.set(new IntWritable(sum.intValue()), new Text(names));
  }

  public void set(IntWritable sum, Text names) {
    this.sum = sum;
    this.names = names;
  }

  public IntWritable getSum() {
    return this.sum;
  }

  public Text getNames() {
    return this.names;
  }

  public void addsumNames(SumNames sumNames) {
    this.set(new IntWritable(this.sum.get() + sumNames.getSum().get()),
        new Text(String.valueOf(this.names.toString()) + "\t" + sumNames.getNames().toString()));
  }

  public void write(DataOutput dataOutput) throws IOException {
    this.sum.write(dataOutput);
    this.names.write(dataOutput);
  }

  public void readFields(DataInput dataInput) throws IOException {
    this.sum.readFields(dataInput);
    this.names.readFields(dataInput);
  }

  public String toString() {
    String str = String.valueOf(Integer.toString(this.sum.get())) + this.names.toString();
    return str;
  }

  public int compareTo(SumNames sumNames) {
    int comparison = this.sum.compareTo(sumNames.sum);
    if (comparison != 0) {
      return comparison;
    }
    return this.names.compareTo(sumNames.names);
  }
}

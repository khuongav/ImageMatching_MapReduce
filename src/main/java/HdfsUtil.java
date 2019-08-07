import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public final class HdfsUtil {
  public static final String DEFAULT_FILE_SYSTEM = "fs.defaultFS";
  public static final String URI_FILE_SYSTEM = "hdfs://master:9000";

  public static void copyToHdfs(String localPath, String hdfsPath) throws IOException {
    Configuration conf = new Configuration();
    conf.set(DEFAULT_FILE_SYSTEM, URI_FILE_SYSTEM);
    FileSystem fs = FileSystem.get((Configuration) conf);
    Path fromLocal = new Path(localPath);
    Path toHdfs = new Path(hdfsPath);
    if (!fs.exists(toHdfs)) {
      fs.mkdirs(toHdfs);
    }
    try {
      fs.copyFromLocalFile(fromLocal, toHdfs);
    } finally {
      fs.close();
    }
  }

  public static void moveToHdfs(String localPath, String hdfsPath) throws IOException {
    Configuration conf = new Configuration();
    conf.set(DEFAULT_FILE_SYSTEM, URI_FILE_SYSTEM);
    FileSystem fs = FileSystem.get((Configuration) conf);
    Path fromLocal = new Path(localPath);
    Path toHdfs = new Path(hdfsPath);
    if (!fs.exists(toHdfs)) {
      fs.mkdirs(toHdfs);
    }
    try {
      fs.moveFromLocalFile(fromLocal, toHdfs);
    } finally {
      fs.close();
    }
  }

  public static void removeExistDir(String hdfsPath) throws IOException {
    Configuration conf = new Configuration();
    conf.set(DEFAULT_FILE_SYSTEM, URI_FILE_SYSTEM);
    FileSystem fs = FileSystem.get((Configuration) conf);
    if (fs.exists(new Path(hdfsPath))) {
      try {
        fs.delete(new Path(hdfsPath), true);
      } finally {
        fs.close();
      }
    }
  }
}

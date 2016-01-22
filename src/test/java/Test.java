import com.jd.bdp.hdfs.mergefiles.Config;
import com.jd.bdp.hdfs.mergefiles.FileType;

import java.util.UUID;

/**
 * Created by wubiao on 1/15/16.
 */
public class Test {
  public static void main(String[] args) {
    System.out.println(UUID.randomUUID());
    long fd = 4321432L;
    double a = 32432423 / 1024 / 1240;
    System.out.println(String.format("%s%s", fd,a));
  }
}

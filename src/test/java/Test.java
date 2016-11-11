import com.jd.bdp.utils.Utils;
import org.apache.hadoop.fs.Path;

/**
 * Created by wubiao on 1/15/16.
 */
public class Test {
  public static void main(String[] args) {
    String a = "{\"id\": 123,\"name\": \"bill\"}";
    System.out.println(new Path("/tmp/test" + Path.getPathWithoutSchemeAndAuthority(new Path("hdfs://0.0.0.0:9000/user/centos1/warehouse/tmp.db/lzo_test"))));

  }
}

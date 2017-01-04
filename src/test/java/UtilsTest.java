import com.jd.bdp.utils.Utils;
import org.junit.Test;

/**
 * Created by wubiao on 1/4/17.
 */
public class UtilsTest {
  @Test
  public void getUser() {
    System.out.println(Utils.USER);

    assert (Utils.USER.equals("centos1"));
  }

  @Test
  public void makeMergeId() {
    String path = "/tmp/ab";
    String id = Utils.makeMergeId(path);
    System.out.println(id);

  }
}

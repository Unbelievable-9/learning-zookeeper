package info.unbelievable9.zookeeper;

import info.unbelievable9.zookeeper.util.CommonUtil;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.IOException;
import java.util.Properties;

/**
 * 测试基类
 *
 * @author : unbelievable9
 * @date : 2019-04-24
 */
public class ZkRootTest {

    protected Properties properties;

    @BeforeTest
    public void beforeTest() throws IOException, InterruptedException {
        properties = CommonUtil.getProperties();
    }

    @AfterTest
    public void afterTest() throws InterruptedException {
        properties = null;
    }
}

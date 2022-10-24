import org.apache.calcite.clickhouse.ClickhouseConnectionSpec;
import org.apache.calcite.clickhouse.ClickhouseSqlOptimizer;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class TestOptimize
{
    private static final String TEST_HOST = "the_clickhouse_host:8023";
    private static final String TEST_USER = "default";
    private static final String TEST_PASSWORD = "xxxx";
    private static final String TEST_DB = "test_db";

    @Test
    public void test()
            throws Exception
    {
        String file = "test.sql";
        String sql = new String(Files.readAllBytes(Paths.get(TestOptimize.class.getClassLoader().getResource(file).getPath())));
        sql = ClickhouseSqlOptimizer.INSTANCE.optimize(
                new ClickhouseConnectionSpec(
                        "jdbc:clickhouse://" + TEST_HOST + "/" + TEST_DB,
                        TEST_USER,
                        TEST_PASSWORD,
                        "ru.yandex.clickhouse.ClickHouseDriver",
                        TEST_DB
                ),
                sql
        );
        System.out.println(sql);
    }


    private static final String MYSQL_TEST_HOST = "localhost:3306";
    private static final String MYSQL_TEST_USER = "yjk";
    private static final String MYSQL_TEST_PASSWORD = "HAIyi@ficc20220407";
    private static final String MYSQL_TEST_DB = "world";
    private static final String PROPS = "?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
    @Test
    public void testMysql()
            throws Exception
    {

        String sql = "select count(1) from (select * from world.city where (ID =1 or ID=2 or ID=3 or id=4 or id=5) and name='x')t";
//        String sql = "select * from world.city where (ID =1 and name='y') or (ID=2 and name='y') " +
//                "or (ID =3 and name='y') or (ID =3 and name='y')";

//        String sql = "select * from city  left join country on city.CountryCode  = country.Code where city.Name like 'A%' and 1=1";
        System.out.println("原始sql----\n"+sql);
        System.out.println("==================");
        sql = ClickhouseSqlOptimizer.INSTANCE.optimize(
                new ClickhouseConnectionSpec(
                        "jdbc:mysql://" + MYSQL_TEST_HOST + "/" + MYSQL_TEST_DB+PROPS,
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        "com.mysql.cj.jdbc.Driver",
                        MYSQL_TEST_DB
                ),
                sql
        );
        System.out.println(sql);
    }
}

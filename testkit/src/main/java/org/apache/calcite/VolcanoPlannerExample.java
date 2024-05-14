package org.apache.calcite;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ConnectionSpec;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangyin
 * @version 1.0.0 2024/4/8 上午11:24
 * @since 1.0.0
 */
public class VolcanoPlannerExample {

    public static void main(String[] args) {
        ConnectionSpec SCOTT = CalciteAssert.DatabaseInstance.HSQLDB.scott;
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        Map<String, Object> map = new HashMap<>();
        map.put("jdbcDriver", SCOTT.driver);
        map.put("jdbcUser", SCOTT.username);
        map.put("jdbcPassword", SCOTT.password);
        map.put("jdbcUrl", SCOTT.url);
        map.put("jdbcCatalog", SCOTT.catalog);
        map.put("jdbcSchema", SCOTT.schema);
        JdbcSchema schema = JdbcSchema.create(rootSchema, "JDBC_SCOTT", map);
        rootSchema = rootSchema.add("JDBC_SCOTT", schema);
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .programs(Programs.standard()).build();

        RelBuilder relBuilder = RelBuilder.create(config);
        RelNode relNode = relBuilder.scan("EMP")
                .project(relBuilder.field("DEPTNO"), relBuilder.field("ENAME"))
                .filter(relBuilder.equals(relBuilder.field("ENAME"), relBuilder.literal("aaaa")))
                .sort(relBuilder.field("DEPTNO")).build();

        System.out.println(RelOptUtil.toString(relNode));

        VolcanoPlanner planner = (VolcanoPlanner) relNode.getCluster().getPlanner();
        planner.setTopDownOpt(true);
        //获取期望的RelTraitSet，这里是将Convention.None替换成EnumerableConvention
        RelTraitSet desired = relNode.getTraitSet().replace(EnumerableConvention.INSTANCE).simplify();
        //设置根节点，会从根节点开始迭代将所有子节点也注册到planner中
        // planner.setRoot(relNode);
        planner.setRoot(planner.changeTraits(relNode, desired));
        RelNode result = planner.chooseDelegate().findBestExp();

        System.out.println(RelOptUtil.toString(result));
    }
}

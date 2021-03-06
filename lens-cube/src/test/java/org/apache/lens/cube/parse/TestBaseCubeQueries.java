/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.cube.parse;

import static org.apache.lens.cube.parse.CandidateTablePruneCause.CandidateTablePruneCode.MISSING_PARTITIONS;
import static org.apache.lens.cube.parse.CubeTestSetup.*;
import static org.apache.lens.cube.parse.DateUtil.*;
import static org.apache.lens.cube.parse.TestCubeRewriter.compareQueries;

import static org.apache.hadoop.hive.ql.parse.HiveParser.KW_AND;

import static org.testng.Assert.*;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lens.cube.metadata.UpdatePeriod;
import org.apache.lens.server.api.error.LensException;

import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import org.antlr.runtime.CommonToken;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import lombok.Getter;

public class TestBaseCubeQueries extends TestQueryRewrite {
  @Getter
  private Configuration conf;
  private final String cubeName = CubeTestSetup.BASE_CUBE_NAME;

  @BeforeTest
  public void setupDriver() throws Exception {
    conf = new Configuration();
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C1,C2");
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AUTO_JOINS, false);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_SELECT_TO_GROUPBY, true);
    conf.setBoolean(CubeQueryConfUtil.ENABLE_GROUP_BY_TO_SELECT, true);
    conf.setBoolean(CubeQueryConfUtil.DISABLE_AGGREGATE_RESOLVER, false);
  }

  @Test
  public void testColumnErrors() throws Exception {
    SemanticException e;

    e = getSemanticExceptionInRewrite("select msr11 + msr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    assertEquals(e.getCanonicalErrorMsg().getErrorCode(),
      ErrorMsg.EXPRESSION_NOT_IN_ANY_FACT.getErrorCode());
    // no fact has the all the dimensions queried
    e = getSemanticExceptionInRewrite("select dim1, stateid, msr3, msr13 from basecube" + " where " + TWO_DAYS_RANGE,
      conf);
    assertEquals(e.getCanonicalErrorMsg().getErrorCode(),
      ErrorMsg.NO_CANDIDATE_FACT_AVAILABLE.getErrorCode());
    PruneCauses.BriefAndDetailedError pruneCauses = extractPruneCause(e);
    String regexp = String.format(CandidateTablePruneCause.CandidateTablePruneCode.COLUMN_NOT_FOUND.errorFormat,
      "Column Sets: (.*?)", "queriable together");
    Matcher matcher = Pattern.compile(regexp).matcher(pruneCauses.getBrief());
    assertTrue(matcher.matches(), pruneCauses.getBrief());
    assertEquals(matcher.groupCount(), 1);
    String columnSetsStr = matcher.group(1);
    assertNotEquals(columnSetsStr.indexOf("stateid"), -1);
    assertNotEquals(columnSetsStr.indexOf("msr3, msr13"), -1);
    assertEquals(pruneCauses.getDetails(),
      new HashMap<String, List<CandidateTablePruneCause>>() {
        {
          put("testfact3_base,testfact3_raw_base", Arrays.asList(CandidateTablePruneCause.columnNotFound("stateid")));
          put("testfact_deprecated,testfact2_raw_base,testfact2_base",
            Arrays.asList(CandidateTablePruneCause.columnNotFound("msr3", "msr13")));
        }
      }
    );
  }


  @Test
  public void testCommonDimensions() throws Exception {
    String hqlQuery = rewrite("select dim1, SUM(msr1) from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected =
      getExpectedQuery(cubeName, "select basecube.dim1, SUM(basecube.msr1) FROM ", null, " group by basecube.dim1",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, SUM(msr1), msr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1, SUM(basecube.msr1), basecube.msr2 FROM ", null,
        " group by basecube.dim1", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1, round(sum(basecube.msr2)/1000) FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    compareQueries(hqlQuery, expected);

    hqlQuery =
      rewrite("select booleancut, msr2 from basecube" + " where " + TWO_DAYS_RANGE + " and substrexpr != 'XYZ'", conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND basecube.dim2 != 10 ,"
          + " sum(basecube.msr2) FROM ", null, " and substr(basecube.dim1, 3) != 'XYZ' "
          + "group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    compareQueries(hqlQuery, expected);

    hqlQuery = rewrite("select dim1, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected =
      getExpectedQuery(cubeName, "select basecube.dim1, sum(basecube.msr12) FROM ", null, " group by basecube.dim1",
        getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    compareQueries(hqlQuery, expected);
  }

  @Test
  public void testMultipleFacts() throws Exception {
    String hqlQuery = rewrite("select roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    String expected1 =
      getExpectedQuery(cubeName, "select sum(basecube.msr12) msr12 FROM ", null,
        null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    String expected2 =
      getExpectedQuery(cubeName, "select round(sum(basecube.msr2)/1000) roundedmsr2 FROM ", null,
        null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    String lower = hqlQuery.toLowerCase();
    assertTrue(lower.startsWith("select mq2.roundedmsr2 roundedmsr2, mq1.msr12 msr12 from ")
      || lower.startsWith("select mq1.roundedmsr2 roundedmsr2, mq2.msr12 msr12 from "), hqlQuery);
    assertTrue(lower.contains("mq1 full outer join") && lower.endsWith("mq2"), hqlQuery);
    assertFalse(lower.contains("<=>"), hqlQuery);

    hqlQuery = rewrite("select dim1, roundedmsr2, msr12 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, sum(basecube.msr12) msr12 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, round(sum(basecube.msr2)/1000) roundedmsr2 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.roundedmsr2 roundedmsr2, mq1.msr12 msr12 from ")
      || lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.roundedmsr2 roundedmsr2, mq2.msr12 msr12"
        + " from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);

    // columns in select interchanged
    hqlQuery = rewrite("select dim1, msr12, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, sum(basecube.msr12) msr12 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, round(sum(basecube.msr2)/1000) roundedmsr2 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    lower = hqlQuery.toLowerCase();
    assertTrue(
      lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || lower.startsWith("select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2"
        + " from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);

    // query with 3 fact tables
    hqlQuery = rewrite("select dim1, msr12, roundedmsr2, msr13, msr3 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, sum(basecube.msr12) msr12 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 =
      getExpectedQuery(cubeName,
        "select basecube.dim1 dim1, round(sum(basecube.msr2)/1000) roundedmsr2, max(basecube.msr3) msr3 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    String expected3 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, max(basecube.msr13) msr13 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "c1_testfact3_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    TestCubeRewriter.compareContains(expected3, hqlQuery);
    assertTrue(
      hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, mq1.msr12 msr12,"
          + " mq2.roundedmsr2 roundedmsr2, mq3.msr13 msr13, mq2.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, mq1.msr12 msr12,"
          + " mq3.roundedmsr2 roundedmsr2, mq2.msr13 msr13, mq3.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, mq2.msr12 msr12,"
          + " mq1.roundedmsr2 roundedmsr2, mq3.msr13 msr13, mq1.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, mq2.msr12 msr12,"
          + " mq3.roundedmsr2 roundedmsr2, mq1.msr13 msr13, mq3.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, mq3.msr12 msr12,"
          + " mq1.roundedmsr2 roundedmsr2, mq2.msr13 msr13, mq1.msr3 msr3 from ")
        || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1, mq3.dim1) dim1, mq3.msr12 msr12,"
          + " mq2.roundedmsr2 roundedmsr2, mq1.msr13 msr13, mq2.msr3 msr3 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.contains("mq2 full outer join ")
      && hqlQuery.endsWith("mq3 on mq1.dim1 <=> mq2.dim1 AND mq1.dim1 <=> mq3.dim1"), hqlQuery);

    // query two dim attributes
    hqlQuery = rewrite("select dim1, dim11, msr12, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, basecube.dim11 dim11, sum(basecube.msr12) msr12 FROM ",
        null, " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 =
      getExpectedQuery(cubeName,
        "select basecube.dim1 dim1, basecube.dim11 dim11, round(sum(basecube.msr2)/1000) roundedmsr2 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) dim11,"
        + " mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, coalesce(mq1.dim11, mq2.dim11) dim11,"
        + " mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ")
      && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1 AND mq1.dim11 <=> mq2.dim11"), hqlQuery);

    // no aggregates in the query
    hqlQuery = rewrite("select dim1, msr11, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, basecube.msr11 msr11 FROM ", null, null,
        getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, round(basecube.msr2/1000) roundedmsr2 FROM ", null, null,
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.msr11 msr11, mq2.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.msr11 msr11, mq1.roundedmsr2 roundedmsr2 from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);

    // query with aliases passed
    hqlQuery =
      rewrite("select dim1 d1, msr12 `my msr12`, roundedmsr2 m2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 d1, sum(basecube.msr12) expr2 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 d1, round(sum(basecube.msr2)/1000) m2 FROM ", null,
        " group by basecube.dim1", getWhereForDailyAndHourly2days(cubeName, "C1_testFact1_BASE"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.d1, mq2.d1) d1, mq2.expr2 `my msr12`, mq1.m2 m2 from ")
      ||
      hqlQuery.toLowerCase().startsWith("select coalesce(mq1.d1, mq2.d1) d1, mq1.expr2 `my msr12`, mq2.m2 m2 from "),
      hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.d1 <=> mq2.d1"), hqlQuery);

    // query with non default aggregate
    hqlQuery = rewrite("select dim1, avg(msr12), avg(msr2) from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, avg(basecube.msr12) msr12 FROM ", null,
        " group by basecube.dim1", getWhereForHourly2days(cubeName, "C1_testfact2_raw_base"));
    expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 dim1, avg(basecube.msr2)) msr2 FROM ", null,
        " group by basecube.dim1", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim1, mq2.dim1) dim1, mq2.msr12 msr12, mq1.msr2 msr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim1, mq2.dim1) dim1, mq1.msr12 msr12, mq2.msr2 msr2 from "), hqlQuery);

    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim1 <=> mq2.dim1"),
      hqlQuery);

    // query with join
    hqlQuery = rewrite("select testdim2.name, msr12, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select testdim2.name name, sum(basecube.msr12) msr12 FROM ", " JOIN " + getDbName()
          + "c1_testdim2tbl testdim2 ON basecube.dim2 = " + " testdim2.id and (testdim2.dt = 'latest') ", null,
        " group by testdim2.name", null, getWhereForDailyAndHourly2days(cubeName, "C1_testFact2_BASE"));
    expected2 =
      getExpectedQuery(cubeName, "select testdim2.name name, round(sum(basecube.msr2)/1000) roundedmsr2 FROM ", " JOIN "
          + getDbName() + "c1_testdim2tbl testdim2 ON basecube.dim2 = "
          + " testdim2.id and (testdim2.dt = 'latest') ", null, " group by testdim2.name", null,
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.name, mq2.name) name, mq2.msr12 msr12, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.name, mq2.name) name, mq1.msr12 msr12, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.name <=> mq2.name"),
      hqlQuery);

    // query with denorm variable
    hqlQuery = rewrite("select dim2, msr13, roundedmsr2 from basecube" + " where " + TWO_DAYS_RANGE, conf);
    expected1 =
      getExpectedQuery(cubeName, "select testdim2.id dim2, max(basecube.msr13) msr13 FROM ", " JOIN " + getDbName()
          + "c1_testdim2tbl testdim2 ON basecube.dim12 = " + " testdim2.id and (testdim2.dt = 'latest') ", null,
        " group by testdim2.id", null, getWhereForHourly2days(cubeName, "C1_testFact3_RAW_BASE"));
    expected2 =
      getExpectedQuery(cubeName, "select basecube.dim2 dim2, round(sum(basecube.msr2)/1000) roundedmsr2 FROM ", null,
        " group by basecube.dim2", getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase().startsWith(
      "select coalesce(mq1.dim2, mq2.dim2) dim2, mq2.msr13 msr13, mq1.roundedmsr2 roundedmsr2 from ")
      || hqlQuery.toLowerCase().startsWith(
        "select coalesce(mq1.dim2, mq2.dim2) dim2, mq1.msr13 msr13, mq2.roundedmsr2 roundedmsr2 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.dim2 <=> mq2.dim2"),
      hqlQuery);

    // query with expression
    // TODO allow expression to be answered from denorm columns
    /*
    hqlQuery =
      rewrite(
        "select booleancut, round(sum(msr2)/1000), avg(msr13 + msr14) from basecube" + " where " + TWO_DAYS_RANGE,
        conf);
    expected1 =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND testdim2.id != 10 expr1,"
          + " avg(basecube.msr13 + basecube.msr14) expr3 FROM ", " JOIN " + getDbName()
          + "c1_testdim2tbl testdim2 ON basecube.dim12 = " + " testdim2.id and (testdim2.dt = 'latest') ", null,
        " group by basecube.dim1 != 'x' AND testdim2.id != 10", null,
        getWhereForHourly2days(cubeName, "C1_testfact3_raw_base"));
    expected2 =
      getExpectedQuery(cubeName, "select basecube.dim1 != 'x' AND basecube.dim2 != 10 expr1,"
          + " round(sum(basecube.msr2)/1000) msr2 FROM ", null,
        " group by basecube.dim1 != 'x' AND basecube.dim2 != 10",
        getWhereForHourly2days(cubeName, "C1_testfact1_raw_base"));
    TestCubeRewriter.compareContains(expected1, hqlQuery);
    TestCubeRewriter.compareContains(expected2, hqlQuery);
    assertTrue(hqlQuery.toLowerCase()
      .startsWith("select coalesce(mq1.expr1, mq2.expr1) expr1, mq2.msr2 msr2, mq1.expr3 expr3 from ")
      || hqlQuery.toLowerCase()
        .startsWith("select coalesce(mq1.expr1, mq2.expr1) expr1, mq1.msr2 msr2, mq2.expr3 expr3 from "), hqlQuery);
    assertTrue(hqlQuery.contains("mq1 full outer join ") && hqlQuery.endsWith("mq2 on mq1.expr1 <=> mq2.expr1"),
      hqlQuery);*/
  }

  @Test
  public void testFallbackPartCol() throws Exception {
    Configuration conf = getConfWithStorages("C1,C2,C3,C4");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    String hql, expected;
    // Prefer fact that has a storage with part col on queried time dim
    hql = rewrite("cube select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    expected = getExpectedQuery(BASE_CUBE_NAME, "select sum(basecube.msr12) FROM ", null, null,
      getWhereForDailyAndHourly2days(BASE_CUBE_NAME, "c1_testfact2_base"));
    compareQueries(hql, expected);

    // If going to fallback timedim, and partitions are missing, then error should be missing partition on that
    conf.set(CubeQueryConfUtil.DRIVER_SUPPORTED_STORAGES, "C4");
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, true);
    SemanticException exc =
      getSemanticExceptionInRewrite("cube select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    PruneCauses.BriefAndDetailedError pruneCause = extractPruneCause(exc);
    assertTrue(pruneCause.getBrief().contains("Missing partitions"));
    assertEquals(pruneCause.getDetails().get("testfact2_base").iterator().next().getCause(), MISSING_PARTITIONS);
    assertEquals(pruneCause.getDetails().get("testfact2_base").iterator().next().getMissingPartitions().size(), 1);
    assertEquals(
      pruneCause.getDetails().get("testfact2_base").iterator().next().getMissingPartitions().iterator().next(),
      "ttd:["
        + UpdatePeriod.SECONDLY.format().format(DateUtils.addDays(DateUtils.truncate(TWODAYS_BACK, Calendar.HOUR), -10))
        + ", " + UpdatePeriod.SECONDLY.format().format(DateUtils.addDays(DateUtils.truncate(NOW, Calendar.HOUR), 10))
        + ")");

    // fail on partial false. Should go to fallback column. Also testing transitivity of timedim relations
    conf.setBoolean(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, false);
    hql = rewrite("cube select msr12 from basecube where " + TWO_DAYS_RANGE, conf);
    String dTimeWhereClause = "basecube.d_time >= '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
      getAbsDateFormatString(getDateUptoHours(
        TWODAYS_BACK)))) + "' and "
      + "basecube.d_time < '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
        getAbsDateFormatString(getDateUptoHours(NOW))));
    String pTimeWhereClause = "basecube.processing_time >= '"
      + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
        getAbsDateFormatString(getDateUptoHours(
          DateUtils.addDays(TWODAYS_BACK, -5))))) + "' and "
        + "basecube.processing_time < '" + HIVE_QUERY_DATE_PARSER.get().format(ABSDATE_PARSER.get().parse(
          getAbsDateFormatString(getDateUptoHours(DateUtils.addDays(NOW, 5)))));
    expected = getExpectedQuery(BASE_CUBE_NAME, "select sum(basecube.msr12) FROM ", null,
        " and " + dTimeWhereClause + " and " + pTimeWhereClause,
      getWhereForDailyAndHourly2daysWithTimeDim(BASE_CUBE_NAME, "ttd",
        DateUtils.addDays(TWODAYS_BACK, -10), DateUtils.addDays(NOW, 10), "c4_testfact2_base"));
    compareQueries(hql, expected);

    // Multiple timedims in single query. test that
    CubeQueryContext ctx =
      rewriteCtx("cube select msr12 from basecube where " + TWO_DAYS_RANGE + " and " + TWO_DAYS_RANGE_TTD, conf);
    assertEquals(ctx.getCandidateFactSets().size(), 1);
    assertEquals(ctx.getCandidateFactSets().iterator().next().size(), 1);
    CandidateFact cfact = ctx.getCandidateFactSets().iterator().next().iterator().next();
    assertEquals(cfact.getRangeToWhereClause().size(), 2);
    for(Map.Entry<TimeRange, String> entry: cfact.getRangeToWhereClause().entrySet()) {
      if (entry.getKey().getPartitionColumn().equals("dt")) {
        ASTNode parsed = HQLParser.parseExpr(entry.getValue());
        assertEquals(parsed.getToken().getType(), KW_AND);
        assertTrue(entry.getValue().substring(((CommonToken) parsed.getToken()).getStopIndex() + 1).toLowerCase()
          .contains(dTimeWhereClause));
        assertFalse(entry.getValue().substring(0, ((CommonToken) parsed.getToken()).getStartIndex()).toLowerCase()
          .contains("and"));
      } else if (entry.getKey().getPartitionColumn().equals("ttd")) {
        assertFalse(entry.getValue().toLowerCase().contains("and"));
      } else {
        throw new LensException("Unexpected");
      }
    }
  }
}

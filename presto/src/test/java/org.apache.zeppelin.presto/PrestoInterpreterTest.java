package org.apache.zeppelin.presto;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class PrestoInterpreterTest {
    private static final int DEFAULT_LIMIT = PrestoInterpreter.DEFAULT_LIMIT_ROW;
    private static final String LIMIT_QUERY_HEAD = PrestoInterpreter.LIMIT_QUERY_HEAD;
    private static final String LIMIT_QUERY_TAIL = PrestoInterpreter.LIMIT_QUERY_TAIL + DEFAULT_LIMIT;

    private PrestoInterpreter interpreter;

    @Before
    public void setup() {
        Properties property = new Properties();
        this.interpreter = new PrestoInterpreter(property);
    }

    @Test
    public void givenWithoutLimitClause_whenAddLimitClause_thenReturnSqlQueryWithDefaultLimit() throws Exception {
        //given
        String sqlWithoutLimit = "SELECT * FROM test";
        String expect = LIMIT_QUERY_HEAD + sqlWithoutLimit + LIMIT_QUERY_TAIL;

        //when
        String actual = interpreter.addLimitClause(sqlWithoutLimit).getQuery();

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is(expect));
    }

    @Test
    public void givenWithValidLimitClause_whenAddLimitClause_thenReturnSqlQueryWithLimit() throws Exception {
        //given
        String sqlWithValidLimit = "SELECT * FROM test LIMIT 100";

        //when
        String actual = interpreter.addLimitClause(sqlWithValidLimit).getQuery();

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is(sqlWithValidLimit));
    }

    @Test
    public void givenWithInvalidLimitClause_whenAddLimitClause_thenReturnSqlQueryWithDefaultLimit() throws Exception {
        //given
        String sqlWithInvalidLimit = "SELECT * FROM test LIMIT 59999999";
        String expect = LIMIT_QUERY_HEAD + sqlWithInvalidLimit + LIMIT_QUERY_TAIL;

        //when
        String actual = interpreter.addLimitClause(sqlWithInvalidLimit).getQuery();

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is(expect));
    }

    @Test
    public void givenNotSelectQuery_whenAddLimitClause_thenReturnGivenQueryTest() {
        //given
        String notSelectQuery = "DESC testABC";

        //when
        String actual = interpreter.addLimitClause(notSelectQuery).getQuery();

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(StringUtils.lowerCase(notSelectQuery), is("desc testabc"));
    }

    @Test
    public void givenInvalidQuery_whenAddLimitClause_thenReturnGivenQueryTest() {
        //given
        String invalidQuery = "elect fro aa";

        //when
        String actual = interpreter.addLimitClause(invalidQuery).getQuery();

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is(invalidQuery));
    }


    @Test
    public void givenCommentQuery_whenAddLimitClause_thenReturnGivenQueryTest() {
        //given
        String invalidQuery = "select * from sbbi.dim_rgn_cd -- limit 100";
        String expect = LIMIT_QUERY_HEAD + invalidQuery + LIMIT_QUERY_TAIL;

        //when
        String actual = interpreter.addLimitClause(invalidQuery).getQuery();

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is(expect));
    }
}
package org.apache.zeppelin.presto;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class PrestoInterpreterTest {

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

        //when
        String actual = interpreter.addLimitClause(sqlWithoutLimit);

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is("select * from test limit 100000"));
    }

    @Test
    public void givenWithValidLimitClause_whenAddLimitClause_thenReturnSqlQueryWithLimit() throws Exception {
        //given
        String sqlWithValidLimit = "SELECT * FROM test LIMIT 100";

        //when
        String actual = interpreter.addLimitClause(sqlWithValidLimit);

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is("select * from test limit 100"));
    }

    @Test
    public void givenWithInvalidLimitClause_whenAddLimitClause_thenReturnSqlQueryWithDefaultLimit() throws Exception {
        //given
        String sqlWithInvalidLimit = "SELECT * FROM test LIMIT 59999999";

        //when
        String actual = interpreter.addLimitClause(sqlWithInvalidLimit);

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is("select * from test limit 100000"));
    }

    @Test
    public void givenNotSelectQuery_whenAddLimitClause_thenReturnGivenQueryTest() {
        //given
        String notSelectQuery = "DESC testABC";

        //when
        String actual = interpreter.addLimitClause(notSelectQuery);

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(StringUtils.lowerCase(notSelectQuery), is("desc testabc"));
    }

    @Test
    public void givenInvalidQuery_whenAddLimitClause_thenReturnGivenQueryTest() {
        //given
        String invalidQuery = "elect fro aa";
        String expect = invalidQuery + " limit 100000";

        //when
        String actual = interpreter.addLimitClause(invalidQuery);

        //then
        Assert.assertThat(actual, is(notNullValue()));
        Assert.assertThat(actual, is(expect));
    }
}
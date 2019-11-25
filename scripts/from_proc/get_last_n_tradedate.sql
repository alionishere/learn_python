CREATE OR REPLACE FUNCTION GET_LAST_N_TRADEDATE(
                                                p_origindate in char, --日期
                                                p_daynum in int default 1 --天数
                                                )
 RETURN char
 IS
  --
  --   模块编号：
  --   模块名称：公共函数 —取最近的第n个交易日
  --   编写人员:
  --
  v_lastndate char(8);
  v_basedate char(8);
  v_basedays int;
  v_daynum int;
BEGIN
  select decode(NVL(max(date_flag), '-'),
                '0',
                decode(abs(p_daynum), 0, 1, abs(p_daynum)),
                abs(p_daynum) + 1)
    into v_daynum
    from params.tb_exchange_date
   where tradedate = p_origindate
     and market_code = '1';
  if p_daynum >= 0 then
    select a.tradedate
      into v_lastndate
      from (select tradedate, row_number() over(order by tradedate) as rn
              from params.tb_exchange_date
             where date_flag = '1'
               and market_code = '1'
               and tradedate >= p_origindate) a
     where a.rn = v_daynum;
  else
    select a.tradedate
      into v_lastndate
      from (select tradedate,
                   row_number() over(order by tradedate desc) as rn
              from params.tb_exchange_date
             where date_flag = '1'
               and market_code = '1'
               and tradedate <= p_origindate) a
     where a.rn = v_daynum;
  end if;
  RETURN v_lastndate;
END;
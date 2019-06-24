sql =
        new StringBuilder(
            "SELECT ORDER_TYPE_ID,DESCRIPTION FROM HER_ORDER_TYPE WITH(NOLOCK) ORDER BY DESCRIPTION");
    rs = this.dao.Exce_Select(sql);
    while (rs.next()) {
      orderDataBean.getHighendOrderTypes().put(StringUtil.validate(rs.getString("ORDER_TYPE_ID")),
          StringUtil.validate(rs.getString("DESCRIPTION")));
    }

    orderDataBean.setSide("");

    return orderDataBean;
  }

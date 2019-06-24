package com.meds.order.logic;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.trimToEmpty;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import javax.servlet.http.HttpSession;
import javax.sql.RowSet;
import org.apache.commons.lang.StringUtils;
import com.audit.AuditCookies;
import com.audit.AuditOrder;
import com.bonafide.commons.BigDecimals;
import com.dme.dao.OrderDao;
import com.eno.util.DateUtil;
import com.meds.admin.data.WebBean;
import com.meds.billing.data.TaxComponentBean;
import com.meds.billing.logic.BillMng;
import com.meds.claim.logic.ClaimValidation;
import com.meds.claim.logic.InsuranceUtil;
import com.meds.claim.logic.InvDtlBalUtil;
import com.meds.claim.logic.ModifierUtil;
import com.meds.cust.logic.CommonFunctions;
import com.meds.cust.logic.CustomerCreditCardUtil;
import com.meds.cust.logic.CustomerUtil;
import com.meds.exception.MedeqException;
import com.meds.form.logic.FormTrackUtil;
import com.meds.hospice.logic.HospiceSync;
import com.meds.order.data.OrderDataBean;
import com.meds.order.data.POSplitDataBean;
import com.meds.order.data.PaXListOfHcpcs;
import com.meds.pchrcv.logic.DropShipPO;
import com.meds.posting.data.InvBean;
import com.meds.posting.logic.AcctUtil;
import com.meds.support.logic.DefaultUtil;
import com.meds.tbl.TblCUSTOMER;
import com.meds.tbl.TblGCHDR;
import com.meds.tbl.TblHCPCS_SPAN;
import com.meds.tbl.TblHER_ORDER;
import com.meds.tbl.TblINVTORY;
import com.meds.tbl.TblNOTESDTL;
import com.meds.tbl.TblORDAUTH;
import com.meds.tbl.TblORDDTL;
import com.meds.tbl.TblORDDTLBREAKOUT;
import com.meds.tbl.TblORDDTL_HIST;
import com.meds.tbl.TblORDHDR;
import com.meds.tbl.TblPA;
import com.meds.tbl.TblPOSPAYMENT;
import com.meds.tbl.TblPOSPLIT;
import com.meds.tbl.TblPREORDAUTH;
import com.meds.tbl.TblPREORDDTL;
import com.meds.tbl.TblPREORDHDR;
import com.meds.tbl.TblPYMT;
import com.meds.tbl.TblREQFILE;
import com.meds.tbl.TblTEMPEDITQTY;
import com.meds.tbl.TblTEMPORD;
import com.meds.tbl.TblTENMONTHANSWER;
import com.meds.tbl.TblTH;
import com.meds.tbl.TblTITLE;
import com.meds.tbl.TblTRACKDTL;
import com.meds.tbl.TblTRACKDTLBREAKOUT;
import com.meds.util.AllowableUtil;
import com.meds.util.ArithmeticUtil;
import com.meds.util.CompanyUtil;
import com.meds.util.Constant;
import com.meds.util.CreditCardUtil;
import com.meds.util.DAOException;
import com.meds.util.DaoUtil;
import com.meds.util.DateTimeUtil;
import com.meds.util.InventoryUtil;
import com.meds.util.ItemUtil;
import com.meds.util.JustAWarning;
import com.meds.util.LogicUtil;
import com.meds.util.MedeqTableUtil;
import com.meds.util.PriceUtil;
import com.meds.util.StringUtil;

import com.meds.vo.VOORDDTL_HIST;
import com.meds.vo.VOORDHDR;
import com.meds.vo.VOORDHDRCommon;
import com.meds.vo.VOPA;
import com.meds.vo.VOPOSPAYMENT;
import com.meds.vo.VOPREORDAUTH;
import com.meds.vo.VOPREORDDTL;
import com.meds.vo.VOPREORDHDR;
import com.meds.vo.VOPYMT;
import com.meds.vo.VOREQFILE;
import com.meds.vo.VOTEMPORD;
import com.meds.vo.VOTRACKDTL;
import com.meds.vp.LogAuditor;

public class OrderDispatchFunctions extends LogicUtil {
  private static final Logger LOGGER = Logger.getLogger(OrderDispatchFunctions.class.getName());

  private MedeqTableUtil medeqTableUtil = null;
  private OrderValidation orderValidation = null;
  private AuditCookies auditCookies = null;

  public OrderDispatchFunctions(HttpSession h, DaoUtil d) {
    super(h, d);
    this.medeqTableUtil = new MedeqTableUtil(this.dao);
    this.orderValidation = new OrderValidation(this.dao, this.sOfficeNo);
    auditCookies = new AuditCookies(h);
  }

  public void changePrices(OrderDataBean orderDataBean, boolean billPeriodChanged)
      throws SQLException, MedeqException {
    List<String> vCriteria = new ArrayList<String>();
    vCriteria.clear();
    vCriteria.add("ORDERNO = " + orderDataBean.getTempOrderNo());
    List<VOTEMPORD> vTempOrd = TblTEMPORD.getVOVector(this.dao, vCriteria);

    }
  }

  public void packageDefaultPrice(PriceUtil price, String sDefaultPrice, int packageId)
      throws DAOException, SQLException {
    if (isNotBlank(sDefaultPrice)) {
      price.setUnitPrice(Double.parseDouble(sDefaultPrice));
      price.setSalePrice(Double.parseDouble(sDefaultPrice));
      price.s
    }
  }

  public List<ValidationDataBean> validate(OrderDataBean orderDataBean) throws Exception {
    // Case: 28429
    // Credit Limit Exception
        if (!orderDataBean.isPOS() && !orderDataBean.isPreAuth()) {
          DefaultUtil df = new DefaultUtil(this.hs, this.dao);
          String creditLimitCompareWith =
              StringUtils.defaultString(df.getParameter("creditLimitCompare"));
          BigDecimal creditLimit = null;
          BigDecimal billed = BigDecimal.ZERO;
          StringBuilder sql = new StringBuilder(
              "SELECT TOP 1 CREDITLIMIT FROM CUSTOMER WITH(NOLOCK) WHERE CUSTOMERID = '")
                  .append(orderDataBean.getCustomerID()).append("'");
          RowSet rs = dao.Exce_Select(sql);
          if (rs.next()) {
            creditLimit = rs.getBigDecimal("CREDITLIMIT");

            if (creditLimit.compareTo(Constant.ZERO) > 0) {
              if (creditLimitCompareWith.equalsIgnoreCase("PATIENT BALANCE")) {
                sql = new StringBuilder("");
                sql.append("SELECT SUM(CASE WHEN B.PRIBAL=0 AND B.SECBAL=0 AND B.TERBAL=0 THEN B.BAL_AMT ELSE B.CUSTRESP_AMT END) AS X ");
                sql.append("FROM INVHDR A, INVDTL B WHERE A.INVOICENO=B.INVOICENO AND A.DEL <> 'Y' AND A.BILLTO = '");
                sql.append(orderDataBean.getBillID()).append("' ");

                BigDecimal balance = BigDecimals.ZERO;
                RowSet rs1 = dao.Exce_Select(sql);
                if (rs1.next()) {
                  balance = rs1.getBigDecimal("X") == null ? BigDecimals.ZERO : rs1.getBigDecimal("X");
                }

                BigDecimal patResp = BigDecimals.ZERO;
                sql = new StringBuilder("SELECT SUM(PATRESP) AS PATRESP FROM TEMPORD WHERE ORDERNO = ").append(orderDataBean.getTempOrderNo());
                RowSet rs2 = dao.Exce_Select(sql);
                if (rs2.next()) {
                  patResp = rs2.getBigDecimal("PATRESP") == null ? BigDecimals.ZERO : rs2.getBigDecimal("PATRESP");
                }

                BigDecimal total = balance.add(patResp);
                if (total.compareTo(creditLimit) > 0) {
                  throw new JustAWarning("Customer exceeded his/her credit limit of (" + creditLimit
                      + "). Customer has a current patient balance of (" + balance
                      + ") and patient resp. of (" + patResp + ") for this order. To continue saving the order, the current patient balance plus patient resp. of this order should not exceed the credit limit.");
                }

              } else {
                sql = new StringBuilder(
                    "SELECT SUM(AMOUNT+TAX) AS X FROM INVDTL A WITH(NOLOCK), ORDDTL B WITH(NOLOCK) WHERE A.ORDDTLID = B.SYSTEMGENERATEID AND B.ORDERNO = ")
                        .append(orderDataBean.getFillOrderNo());
                rs = dao.Exce_Select(sql);
                if (rs.next() && rs.getBigDecimal("X") != null) {
                  billed = rs.getBigDecimal("X");
                }
                sql = new StringBuilder(
                    "SELECT SUM(BAL_AMT) AS X FROM INVHDR WITH(NOLOCK) WHERE BILLTO = '")
                        .append(orderDataBean.getBillID()).append("' AND DEL <> 'Y'");
                rs = dao.Exce_Select(sql);
                if (rs.next()) {
                  BigDecimal balance = rs.getBigDecimal("X");

                  if (balance != null) {
                    BigDecimal total = balance.add(new BigDecimal(orderDataBean.getSubTotal()))
                        .add(new BigDecimal(orderDataBean.getTax())).subtract(billed);
                    if (total.compareTo(creditLimit) > 0) {
                      throw new JustAWarning("Customer exceeded his/her credit limit of(" + creditLimit
                          + "). Customer has a current invoice balance of (" + balance
                          + "). To continue saving the order, the current invoice balance plus order total should not exceed the credit limit.");
                    }
                  }
                }
              }
            }
          }
        }

    orderDataBean.setTempOrdSeqNo(this.dao);

    List<String> vCriteria = new ArrayList<String>();
    vCriteria.clear();
    vCriteria.add("ORDERNO = " + orderDataBean.getTempOrderNo());

    List<String> vOrder = new ArrayList<String>();
    vOrder.clear();
    vOrder.add("SEQNO ASC");

    List<? extends VOORDDTLCommon> vTempOrd = TblTEMPORD.getVOVector(this.dao, vCriteria, vOrder);
    VOORDHDRCommon voOrdHdr = orderDataBean.getOrdHdr(this.dao, this.local, this.sOfficeNo, this.sEmployeeNo,
        vTempOrd.size(), this.isZeroBalance(orderDataBean));
    voOrdHdr.setORDERNO(orderDataBean.getFillOrderNo());

    boolean isSfosmOrder = orderDataBean.isSfosm() && !orderDataBean.isPOS();
    List<ValidationDataBean> missing = this.orderValidation.validate(orderDataBean.isPDP(), isSfosmOrder, voOrdHdr,
        orderDataBean.getOrdAuth(this.dao), vTempOrd, orderDataBean.getPoLists(),
        toPaList(orderDataBean.getPreAuthorizations()), orderDataBean.getHighendDates(),
        orderDataBean.getTempOrderNo(), this.sEmployeeNo);

    keepValidationRecord(orderDataBean, missing);

    return missing;
  }

  private List<VOPA> toPaList(List<PaXListOfHcpcs> preAuthorizations) {
    List<VOPA> pas = new ArrayList<VOPA>();
    if (preAuthorizations != null && preAuthorizations.size() > 0) {
      for (PaXListOfHcpcs paXListOfHcpcs : preAuthorizations) {
        pas.add(paXListOfHcpcs.getPa());
      }
    }
    return pas;
  }

  private void keepValidationRecord(OrderDataBean orderDataBean, List<ValidationDataBean> missing)
      throws DAOException, SQLException {
    if (!orderDataBean.isPreAuth() && orderDataBean.isClaim()) {
      deleteIncomplete(orderDataBean.getFillOrderNo(), orderDataBean.getTempOrderNo());

      if (missing.size() > 0) {
        insertIncomplete(missing, orderDataBean.getFillOrderNo(), orderDataBean.getTempOrderNo());
      }
    }
  }

  private boolean isZeroBalance(OrderDataBean orderDataBean) throws DAOException, SQLException {
    StringBuilder sql = new StringBuilder(
        "SELECT UNITPRICE FROM TEMPORD WITH(NOLOCK) WHERE UNITPRICE > 0 AND ORDERNO = ")
            .append(orderDataBean.getTempOrderNo());
    return !dao.Exce_Select(sql).next();
  }

  public List<ValidationDataBean> save(OrderDataBean orderDataBean) throws Exception {
    return save(false, orderDataBean);
  }

  public List<ValidationDataBean> save(boolean isForceSave, OrderDataBean orderDataBean) throws Exception {
    pdp(orderDataBean);

    orderDataBean.setTempOrdSeqNo(this.dao);

    List<String> vCriteria = new ArrayList<String>();
    vCriteria.clear();
    vCriteria.add("ORDERNO = " + orderDataBean.getTempOrderNo());

    List<String> vOrder = new ArrayList<String>();
    vOrder.clear();
    vOrder.add("SEQNO ASC");

    List<? extends VOORDDTLCommon> vTempOrd = TblTEMPORD.getVOVector(this.dao, vCriteria, vOrder);
    if (!isForceSave) {

      List<ValidationDataBean> missing = this.validate(orderDataBean);
      if (missing.size() > 0) {
        return missing;
      }
    }

    this.sale2RentalUpdate(orderDataBean);

    int iOrderNo = 0;

    // Get order number
    if (orderDataBean.getEdit().equals("Y")) {
      iOrderNo = orderDataBean.getFillOrderNo();
    } else {
      iOrderNo = orderDataBean.getPreAuth().equals("Y")
          ? this.medeqTableUtil.getSeqNumber("PREORDERNO", this.sEmployeeNo)
          : this.medeqTableUtil.getSeqNumber("ORDERNO", this.sEmployeeNo);
    }

    orderDataBean.setOrderNo(iOrderNo);

    /*
     * Updates orderIncomplete after we retrieved real orderNo
     */
    updateOrderIncomplete(orderDataBean);

    log(orderDataBean);

    // Save to PYMT 02/14/06
    this.savePYMT(orderDataBean);

    // Update preauth status
    this.updatePreAuthStatus(orderDataBean);

    this.clean(orderDataBean.getOrderNo(), orderDataBean.getTempOrderNo(), orderDataBean.getCustomerID());

    // NEW AUDIT TRAIL
    String sIPAddr = auditCookies.getIPAddress();
    String sUserAgent = auditCookies.getUserAgent();
    AuditOrder auditOrder = new AuditOrder(orderDataBean.getCompanyNo(), this.sEmployeeNo, sIPAddr, sUserAgent, iOrderNo);
    VOORDHDR oldOrdHdr = auditOrder.getOrderHeader(this.dao);

    // Save ORDHDR Table
    VOORDHDRCommon newOrdHdr = this.saveOrdHdr(orderDataBean, vTempOrd, this.isZeroBalance(orderDataBean));

    // NEW AUDIT TRAIL
    if((oldOrdHdr != null) && (newOrdHdr != null)) {
      auditOrder.writeOrderHeaderDiff(oldOrdHdr, newOrdHdr);
    }

    // ORDAUTH Table
    if (orderDataBean.getFileClaim().equals("Y") || orderDataBean.isEnableClaimTab()) {
      this.saveOrdAuth(orderDataBean);
    }

    // High-end
    if (!orderDataBean.isPreAuth() && orderDataBean.getHighendDates().size() > 0) {
      if (orderDataBean.isEditing()) {
        dao.Exce_Update_Del_ins("DELETE HER_ORDER WHERE ORDERNO = " + iOrderNo);
      }

      for (VOHER_ORDER voHerOrder : orderDataBean.getHighendDates()) {
        voHerOrder.setORDERNO(iOrderNo);
        TblHER_ORDER.insert(dao, voHerOrder);
      }
    }

    // Order Rules
    if (!orderDataBean.isPreAuth()) {
      OrderRuleUtil.assignByOrderNo(dao, iOrderNo);
    }

    // PA Table
    this.savePA(orderDataBean);

    // PA_X_HCPCS Table
    this.savePA_X_HCPCS(orderDataBean);

    if (orderDataBean.getEdit().equals("Y") && !orderDataBean.getPreAuth().equals("Y")) {
      // REN1 -> SALE
      for (int i = 0; i < orderDataBean.getTempSeqNo() - 1; i++) {
        VOTEMPORD voTempOrd = TblTEMPORD.getVO(this.dao, orderDataBean.getTempOrderNo(), i + 1);
        VOORDDTL voOrdDtlOriginal = TblORDDTL.getVOByIdentity(this.dao, voTempOrd.getORDDTLID());

        if (voTempOrd.getORDDTLID() != 0 && voOrdDtlOriginal.isRecordExist()
            && voOrdDtlOriginal.getITEMTYPE().equals("REN1") && voTempOrd.getITEMTYPE().equals("SALE")) {
          this.rentalToSaleRoutine(voOrdDtlOriginal, false);
        }
      }

      // update ORDDTL_HIST
      this.backupOrdDtl2OrdDtl_Hist(orderDataBean.getOrderNo());
    }

    // ORDDTL table
    this.saveOrdDtl(orderDataBean, vTempOrd, auditOrder);

    poSplit(orderDataBean);

    deleteTempOrdTempEditQty();

    dropshipPo(orderDataBean);

    if (this.sCompanyNo.startsWith("GENPAC")
        || "Y".equals(new DefaultUtil(this.hs, this.dao).getParameter("GENPAC"))) {
      if (isBlank(orderDataBean.getHoldReason()) && !hasDelivery(orderDataBean)) {
        LOGGER.info("GENPAC special logic kicks in, orderNo: " + orderDataBean.getOrderNo());
        new POS(this.hs, this.dao).save(orderDataBean, false);
      }
    } else {
      // Point of sale saving part
      if (orderDataBean.getPOS().equals("Y")) {
        new POS(this.hs, this.dao).save(orderDataBean, false);
      }
    }

    updateOrdDtlSchedule(orderDataBean.getOrderNo());

    // Assign order
    new Assign(hs, dao).assign(orderDataBean);

    if (orderDataBean.getEdit().equals("Y") && !orderDataBean.getPreAuth().equals("Y")) {
      new BillMng(this.hs, this.dao).rebuildBILLABLETRACK(orderDataBean.getOrderNo());
    }

    if (!orderDataBean.getEdit().equals("Y") && !orderDataBean.getPreAuth().equals("Y")) {
      new com.meds.recur.logic.RcrMng(this.hs, this.dao).rebuildRXDUE(orderDataBean.getOrderNo());
    }

    // Revalidate claim
    if (orderDataBean.getEdit().equals("Y")) {
      new ClaimValidation(this.hs, this.dao, false, false).revalidateByOrderNo(orderDataBean.getOrderNo());
    }

    updateInvoice(orderDataBean);

    updateDescription(orderDataBean);

    updatePharmacy(orderDataBean);

    // For IVF
    StringBuilder sql = new StringBuilder("UPDATE INSVERIFY SET ORDERNO = ").append(orderDataBean.getOrderNo())
        .append(" WHERE ORDERNO = ").append(orderDataBean.getTempOrderNo() * -1);
    this.dao.Exce_Update_Del_ins(sql);

    // Credit cards
    CustomerCreditCardUtil.save(this.dao, this.sCompanyNo, this.sEmployeeNo, orderDataBean.getCreditCards(),
        orderDataBean.getCustomerID());

    // Split
    if (orderDataBean.isShowOrderSplit()) {
      new BananaSplit(this.hs, this.dao).split(orderDataBean.getOrderNo());
    }

    // for FITTING_EVALUATION_FORM
    this.updateFITTING_EVALUATION_FORM(orderDataBean);

    recalculateBalance(orderDataBean);

    updateHOrdhdr(orderDataBean);

    autoInsertFormTrack(orderDataBean.getOrderNo(), orderDataBean.isPreAuth());

    if (!orderDataBean.isPreAuth()) {
      boolean isSendEmail = !orderDataBean.isNoEmail();
      new HospiceSync(hs, dao).syncOrderFromDme(orderDataBean.getOrderNo(), false, isSendEmail);
    }

    //MED-2131 AWP Custom Request - Order Quota per Patient per Quarter
    if (CompanyUtil.isAWP(dao.getCompanyNo())) {
      if (!orderDataBean.getEdit().equals("Y") && !orderDataBean.getPreAuth().equals("Y") && !orderDataBean.getPOS().equals("Y")) {
        addCustomerAlert(orderDataBean.getCustomerID(), orderDataBean.getOrderDate());
      }
    }

    return null;
  }

  private void addCustomerAlert(String customerId, String orderDate) throws SQLException, DAOException {
    Formatter f = new Formatter();
    f.format("UPDATE CUSTOMER SET ALERTREASON='HAS BEEN PROCESSED ON %s', ALERT=2 WHERE CUSTOMERID = '%s'", orderDate,
      customerId);
    this.dao.Exce_Update_Del_ins(f.toString());
  }

  private void autoInsertFormTrack(int orderNo, boolean preauth) throws DAOException, SQLException {
    if (orderNo > 0 && FormTrackUtil.isLmnByHcpcs(this.sCompanyNo)) {
      String prefix = "1914" + (preauth ? "P" : "O");
      Formatter f = new Formatter();
      f.format(
          "INSERT INTO FORMTRACK(EVENTTYPE,EVENTNO,FORMNO,BARCODE,AUDIT_USER,AUDIT_DATE,AUDIT_TIME,PHYORDSEQ,STARTDATE,ORIGINALORDER,HCPCS)");
      f.format("SELECT 'OR',ORDERNO,'1117',");
      f.format("'%s'+CONVERT(VARCHAR(50),ORDERNO)+'1117'+CONVERT(VARCHAR(50),X.PHYORDSEQ)+X.HCPCS,", prefix);
      f.format(" %s,", this.sEmployeeNo);
      f.format(" CONVERT(VARCHAR(8),GETDATE(),112),REPLACE(CONVERT(VARCHAR(8),GETDATE(),108),':',''),");
      f.format(" X.PHYORDSEQ,CONVERT(VARCHAR(8),GETDATE(),112),0,X.HCPCS");
      f.format(" FROM");
      f.format(" (");
      f.format("     SELECT DISTINCT D.ORDERNO,A.PHYORDSEQ,D.HCPCS,F.INSURANCE");
      f.format("     FROM SUBTYPE A WITH(NOLOCK), TITLE C WITH(NOLOCK),ORDDTL D WITH(NOLOCK),");
      f.format("     ORDHDR E WITH(NOLOCK),ORDAUTH F WITH(NOLOCK),FORMINSURANCE G WITH(NOLOCK)");
      f.format("     WHERE C.MAKE=D.MAKE AND C.PARTNO=D.PARTNO");
      f.format("     AND C.SUBTYPE=A.SUBTYPE AND A.PHYORDSEQ>0");
      f.format("     AND D.ORDERNO=E.ORDERNO");
      f.format("     AND E.ORDERNO=F.ORDERNO");
      f.format("     AND F.INSURANCE=G.INSURANCE");
      f.format("     AND D.CLAIM='Y'");
      f.format("     AND E.CLAIMORDER='Y'");
      f.format("     AND G.FORMNO='1117'");
      f.format("     AND D.EXTEND>0 AND D.ORDERNO=%s", orderNo);
      f.format(" ) X LEFT JOIN (");
      f.format("     SELECT EVENTNO,EVENTTYPE,PHYORDSEQ,HCPCS FROM FORMTRACK WITH(NOLOCK)");
      f.format("     WHERE EVENTTYPE='OR' AND FORMNO='1117' AND EVENTNO=%s", orderNo);
      f.format("     UNION");
      f.format("     SELECT EVENTNO,EVENTTYPE,PHYORDSEQ,HCPCS FROM FORMTRACKHIST WITH(NOLOCK)");
      f.format("     WHERE EVENTTYPE='OR' AND FORMNO='1117' AND EVENTNO=%s", orderNo);
      f.format(" ) Y ON X.ORDERNO=Y.EVENTNO AND X.PHYORDSEQ=Y.PHYORDSEQ AND X.HCPCS=Y.HCPCS");
      f.format(" LEFT JOIN (");
      f.format(
          "     SELECT 'CIGNA GOVERNMENT SERVICES JURISDICTION C' INSURANCE,HCPCS FROM HCPCS_CMN WHERE CMN2='CMN4843'");
      f.format(" ) Z ON Z.HCPCS=X.HCPCS AND Z.INSURANCE=X.INSURANCE");
      f.format(" WHERE Y.EVENTNO IS NULL AND Z.HCPCS IS NULL");
      dao.Exce_Update_Del_ins(f.toString());
    }
  }

  private boolean hasDelivery(OrderDataBean orderDataBean) throws DAOException, SQLException {
    if (orderDataBean.isEditing() || orderDataBean.isEditingPos()) {
      Formatter f = new Formatter();
      f.format("SELECT SUM(SHIPQTY-RTNQTY) AS SHIPPEDQTY");
      f.format(" FROM ORDDTL WITH(NOLOCK)");
      f.format(" WHERE ORDERNO=5675", orderDataBean.getOrderNo());

      RowSet rs = dao.Exce_Select(f.toString());
      return rs.next() && rs.getInt("SHIPPEDQTY") > 0;
    }

    return false;
  }

  private void deleteTempOrdTempEditQty() throws DAOException, SQLException {
    // Delete TEMPORD and TEMPEDITQTY
    List<String> vCriteria = new ArrayList<String>();
    vCriteria.add("AUDIT_USER = " + this.sEmployeeNo);
    TblTEMPORD.delete(this.dao, vCriteria);
    TblTEMPEDITQTY.delete(this.dao, vCriteria);
  }

  private void updateOrdDtlSchedule(int orderNo) throws DAOException, SQLException {
    List<String> vCriteria = new ArrayList<String>();
    vCriteria.add("ORDERNO = " + orderNo);
    List<VOORDDTL> vOrdDtl = TblORDDTL.getVOVector(this.dao, vCriteria);
    for (int i = 0; i < vOrdDtl.size(); i++) {
      VOORDDTL voOrdDtl = vOrdDtl.get(i);
      if (StringUtils.isNotBlank(voOrdDtl.getRXTABLE())) {
        vCriteria.clear();
        vCriteria.add("ORDDTLID = 0");
        vCriteria.add("RXSEQNO = '" + voOrdDtl.getRXSEQNO() + "'");
        vCriteria.add("RXITEMSEQNO = '" + voOrdDtl.getRXITEMSEQNO() + "'");

        List<String> vSet = new ArrayList<String>();
        vSet.clear();
        vSet.add("ORDDTLID = " + voOrdDtl.getSYSTEMGENERATEID());

        TblORDDTLBREAKOUT.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);

        StringBuilder sql = new StringBuilder("UPDATE ORDDTLSCHEDULE");
        sql.append(" SET ORDDTLID = O.SYSTEMGENERATEID");
        sql.append(" FROM ORDDTL AS O, ORDDTLSCHEDULE AS S");
        sql.append(" WHERE S.ORDDTLID = 0 AND O.RXSEQNO = S.RXSEQNO AND O.RXITEMSEQNO = S.RXITEMSEQNO");
        this.dao.Exce_Update_Del_ins(sql);
      }
    }
  }

  private void updateInvoice(OrderDataBean orderDataBean) throws DAOException, SQLException {
    // Update invoice
    if (!orderDataBean.getPreAuth().equals("Y") && orderDataBean.getEdit().equals("Y")
        && orderDataBean.isUpdateInvoice()) {
      StringBuilder sql = new StringBuilder("UPDATE INVHDR SET NOTES = '").append(orderDataBean.getSpecialNotes())
          .append("' WHERE ORDERNO = ").append(orderDataBean.getOrderNo());
      this.dao.Exce_Update_Del_ins(sql);
    }
  }

  private void updateDescription(OrderDataBean orderDataBean) throws DAOException, SQLException {
    // Update description
    if (!orderDataBean.getPreAuth().equals("Y") && orderDataBean.getEdit().equals("Y")) {
      StringBuilder sql = new StringBuilder("UPDATE TRACKDTL SET DESCRIPTION = ORDDTL.DESCRIPTION FROM ORDDTL");
      sql.append(" WHERE TRACKDTL.ORDDTLID = ORDDTL.SYSTEMGENERATEID");
      sql.append(" AND TRACKDTL.DESCRIPTION <> ORDDTL.DESCRIPTION");
      sql.append(" AND ORDDTL.ORDERNO = ").append(orderDataBean.getOrderNo());
      this.dao.Exce_Update_Del_ins(sql);
    }
  }

  private void updatePharmacy(OrderDataBean orderDataBean) throws DAOException, SQLException {
    // For pharmacy rx
    if (orderDataBean.isPharmacyRx()) {
      StringBuilder sql = new StringBuilder("UPDATE PHARMACY SET RXSTATUS = 'P', INITORDERNO = ")
          .append(orderDataBean.getOrderNo()).append(" WHERE RXSTATUS = 'T' AND INITORDERNO = ")
          .append(orderDataBean.getTempOrderNo());
      this.dao.Exce_Update_Del_ins(sql);
    }
  }

  private void recalculateBalance(OrderDataBean orderDataBean) throws DAOException, SQLException {
    // Recalculate balances.
    if (orderDataBean.getEdit().equals("Y")) {
      RowSet rsx = this.dao
          .Exce_Select("SELECT INVOICENO FROM INVHDR WITH(NOLOCK) WHERE DEL <> 'Y' AND ORDERNO = "
              + orderDataBean.getOrderNo() + " ORDER BY INVOICENO");
      while (rsx.next()) {
        new InvDtlBalUtil(this.hs, this.dao).calculateBalance(rsx.getInt("INVOICENO"));
      }
    }
  }

  private void updateHOrdhdr(OrderDataBean orderDataBean) throws DAOException, SQLException {
    Formatter f = new Formatter();
    f.format("UPDATE H_ORDHDR");
    f.format(" SET HOSPICEID = A.BILLTOID,");
    f.format(" BTLNAME = C.B2NAME, BTFNAME = C.B2FIRSTNAME,");
    f.format(" BTMNAME = C.B2MIDDLEINITIAL,");
    f.format(" BTPHONE = C.B2PHONE, BTADDRESS1 = C.B2ADDRESS,");
    f.format(" BTADDRESS2 = C.B2ADDRESS2,");
    f.format(" BTCITY = C.B2CITY, BTCOUNTY = C.B2COUNTY,");
    f.format(" BTSTATE = C.B2STATE, BTZIP = C.B2ZIP");
    f.format(" FROM ORDHDR A, H_ORDHDR B, CUSTOMER C");
    f.format(" WHERE B.DMEORDERNO = A.ORDERNO");
    f.format(" AND C.CUSTOMERID = A.BILLTOID");
    f.format(" AND HOSPICEID <> A.BILLTOID");
    f.format(" AND A.ORDERNO = %s", orderDataBean.getOrderNo());
    dao.Exce_Update_Del_ins(f.toString());
  }

  private void dropshipPo(OrderDataBean orderDataBean) throws DAOException, SQLException, Exception {
    // Dropship po
    if (!orderDataBean.getPOS().equals("Y") && !orderDataBean.getPreAuth().equals("Y")) {
      if (orderDataBean.getHighEnd().equals("N") || orderDataBean.getHighEnd().equals("Y")
          && HighEndUtil.getCreatePO(this.dao, orderDataBean.getOrderNo())) {
        new DropShipPO(this.hs, this.dao).create(orderDataBean.getOrderNo());
      }

      // Update OBI orderno
      if (StringUtils.isNotBlank(orderDataBean.getSource())) {
        StringBuilder sql = new StringBuilder("UPDATE OBI SET ORDERNO = ").append(orderDataBean.getOrderNo());
        sql.append(" WHERE SOURCE = '").append(orderDataBean.getSource()).append("' AND TEMPORDERNO = ")
            .append(orderDataBean.getTempOrderNo());
        dao.Exce_Update_Del_ins(sql);
      }

      if (!orderDataBean.isEditing()) {
        StringBuilder sql = new StringBuilder("UPDATE OBI_ORDERSPEC SET ORDERNO = ")
            .append(orderDataBean.getOrderNo());
        sql.append(" WHERE ORDERNO = ").append(orderDataBean.getTempOrderNo() * -1);
        dao.Exce_Update_Del_ins(sql);
      }
    }
  }

  private void poSplit(OrderDataBean orderDataBean) throws DAOException, SQLException {
    // For PO list
    if (!orderDataBean.getEdit().equals("Y") && !orderDataBean.getPOS().equals("Y")
        && !orderDataBean.getPreAuth().equals("Y")) {
      for (int j = 0; j < orderDataBean.getPoLists().size(); j++) {
        List<POSplitDataBean> sublist = orderDataBean.getPoLists().get(j);
        for (POSplitDataBean poSplit : sublist) {
          poSplit.getPOSplit().setPOSEQNO(j + 1);
          TblPOSPLIT.insert(this.dao, poSplit.getPOSplit(), this.today, this.sEmployeeNo);
        }
      }
    }
  }

  private void pdp(OrderDataBean orderDataBean) throws Exception {
    // Case: 28329 fro PDP new PMSID
    if (orderDataBean.isPDP()) {
      RowSet rs = dao
          .Exce_Select("SELECT TOP 1 CUSTOMERID, FIRSTNAME, NAME FROM CUSTOMER WITH(NOLOCK) WHERE ALTID = '"
              + orderDataBean.getPmsID() + "'");

      // Customer does not exist
      if (!rs.next()) {
        String customerID = new CommonFunctions(hs, dao).assignCustomerID(true, "", orderDataBean.getLastName(),
            orderDataBean.getFirstName());

        VOCUSTOMER voCustomer = new VOCUSTOMER();
        voCustomer.setCUSTOMERID(customerID);
        voCustomer.setFIRSTNAME(orderDataBean.getFirstName());
        voCustomer.setNAME(orderDataBean.getLastName());
        voCustomer.setALTID(orderDataBean.getPmsID());

        CustomerUtil.setDefaultValue(dao, sOfficeNo, voCustomer, true);

        TblCUSTOMER.insert(dao, voCustomer);

        orderDataBean.setCustomerID(customerID);
        orderDataBean.setVoCustomer(voCustomer);
      }
    }
  }

  private void savePA_X_HCPCS(OrderDataBean orderDataBean) throws DAOException, SQLException {
    if (orderDataBean.hasPaXHcpcs()) {
      Formatter f1 = new Formatter();
      f1.format("UPDATE PA_X_HCPCS SET DELETED = 'true' WHERE ORDERNO = '%s' AND DELETED <> 'true' ",
          orderDataBean.getOrderNo() * (orderDataBean.isPreAuth() ? -1 : 1));
      this.dao.Exce_Update_Del_ins(f1.toString());

      insertPaXHcpcs(this.dao, orderDataBean.getPreAuthorizations());
    }
  }

  private static void insertPaXHcpcs(DaoUtil dao, List<PaXListOfHcpcs> preAuthorizations)
      throws DAOException, SQLException {
    for (PaXListOfHcpcs paXListOfHcpcs : preAuthorizations) {
      insertPaXHcpcs(dao, paXListOfHcpcs);
    }
  }

  public static void insertPaXHcpcs(DaoUtil dao, PaXListOfHcpcs paXListOfHcpcs) throws DAOException, SQLException {
    VOPA pa = paXListOfHcpcs.getPa();

    if (pa.getSYSID() > 0 && paXListOfHcpcs.getListOfHcpcs() != null
        && paXListOfHcpcs.getListOfHcpcs().size() > 0) {
      for (String hcpcs : paXListOfHcpcs.getListOfHcpcs()) {
        Formatter f2 = new Formatter();
        f2.format(
            "INSERT INTO PA_X_HCPCS([PANO],[HCPCS],[ORDERNO],[AUDIT_DATE],[AUDIT_TIME],[AUDIT_USER],[DELETED],[PASYSID]) ");
        DateTimeUtil today = new DateTimeUtil();
        f2.format("SELECT PANO,'%s',ORDERNO,'%s','%s',%s,0,SYSID FROM PA WHERE SYSID=%s", hcpcs,
            today.getDate(), today.getTime(), pa.getAUDIT_USER(), pa.getSYSID());
        dao.Exce_Update_Del_ins(f2.toString());
      }
    }
  }

  private void updateOrderIncomplete(OrderDataBean orderDataBean) throws DAOException, SQLException {
    if (!orderDataBean.isPreAuth() && orderDataBean.isClaim()) {
      Formatter f = new Formatter();
      f.format("UPDATE OrderIncomplete SET orderNo=%s WHERE tempOrderNo=%s", orderDataBean.getOrderNo(),
          orderDataBean.getTempOrderNo());
      this.dao.Exce_Update_Del_ins(f.toString());
    }
  }

  private void deleteIncomplete(int orderNo, int tempOrderNo) throws DAOException, SQLException {
    Formatter f = new Formatter();
    f.format("DELETE OrderIncomplete WHERE");
    if (orderNo > 0) {
      f.format(" orderNo=%s", orderNo);
    } else {
      f.format(" tempOrderNo=%s", tempOrderNo);
    }

    this.dao.Exce_Update_Del_ins(f.toString());
  }

  private void insertIncomplete(List<ValidationDataBean> missing, int orderNo, int tempOrderNo)
      throws DAOException, SQLException {
    for (ValidationDataBean validationDataBean : missing) {
      Formatter f = new Formatter();
      f.format("INSERT INTO OrderIncomplete(orderNo,tempOrderNo,reasonid,employeeNo)");
      f.format(" VALUES(%s,%s,%s,%s)", orderNo, tempOrderNo, validationDataBean.getReasonId(), this.sEmployeeNo);
      this.dao.Exce_Update_Del_ins(f.toString());
    }
  }

  private void updateFITTING_EVALUATION_FORM(OrderDataBean orderDataBean)
      throws IOException, DAOException, SQLException {

    Map<String, String> map = orderDataBean.getFittingEvaluationForm();

    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bout);
    out.writeObject(map);
    out.close();

    byte[] data = bout.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(data);

    boolean recordExist = checkFITTING_EVALUATION_FORMRecord(orderDataBean.getOrderNo());

    PreparedStatement pstmt = null;
    if (recordExist) {
      if (map.size() > 1) {
        String psSql = "UPDATE FITTING_EVALUATION_FORM SET AUDIT_USER=?,AUDIT_DATE=?,"
            + "AUDIT_TIME=?,FORM_VALUE = ? WHERE ORDERNO = " + orderDataBean.getOrderNo();
        pstmt = dao.getConnection().prepareStatement(psSql);
        pstmt.setInt(1, this.sEmployeeNo);
        pstmt.setString(2, this.today.getDate());
        pstmt.setString(3, this.today.getTime());
        pstmt.setBinaryStream(4, bis, data.length);

        pstmt.executeUpdate();
        pstmt.close();

      } else {
        // If map is empty(only include the saveData button), then
        // delete the record
        this.dao.Exce_Update_Del_ins(
            "DELETE FROM FITTING_EVALUATION_FORM WHERE ORDERNO=" + orderDataBean.getOrderNo());
      }
    } else {
      if (map.size() > 1) {
        String psSql = "INSERT INTO FITTING_EVALUATION_FORM VALUES(?,?,?,?,?)";
        pstmt = dao.getConnection().prepareStatement(psSql);
        pstmt.setBinaryStream(2, bis, data.length);
        pstmt.setInt(1, orderDataBean.getOrderNo());
        pstmt.setInt(3, this.sEmployeeNo);
        pstmt.setString(4, this.today.getDate());
        pstmt.setString(5, this.today.getTime());

        pstmt.executeUpdate();
        pstmt.close();
      }
    }

  }

  private boolean checkFITTING_EVALUATION_FORMRecord(int orderNo) throws DAOException, SQLException {
    return this.dao.Exce_Select("SELECT 1 FROM FITTING_EVALUATION_FORM WITH(NOLOCK) WHERE ORDERNO=" + orderNo)
        .next();
  }

  private void log(OrderDataBean orderDataBean) {

    int iOrderNo = orderDataBean.getOrderNo();

    LogAuditor.newInstance().log(this.sCompanyNo,
        orderDataBean.isPreAuth() ? LogAuditor.Type.PREAUTH : LogAuditor.Type.ORDER,
        orderDataBean.isEditing() ? LogAuditor.Action.UPDATE : LogAuditor.Action.CREATE,
        String.valueOf(iOrderNo), this.sEmployeeNo, "");

    if (!orderDataBean.isEditing()) {
      // NEW AUDIT TRAIL
      String sIPAddr = auditCookies.getIPAddress();
      String sUserAgent = auditCookies.getUserAgent();
      AuditOrder auditOrder = new AuditOrder(this.sCompanyNo, this.sEmployeeNo, sIPAddr, sUserAgent, iOrderNo);
      auditOrder.writeOrderAuditCreate();
        }

  }

  private void saveGiftCertificate(OrderDataBean orderDataBean, VOTEMPORD voTempOrd)
      throws SQLException, DAOException {
    if (!orderDataBean.isEditing() && OrderMiscFunctions.isGiftCertificate(voTempOrd.getSKU())) {
      VOGCHDR voGCHdr = new VOGCHDR();
      // Auto-generated gift certificate number
      if (StringUtils.isBlank(voTempOrd.getLOTNO())) {
        String gcno = String.valueOf(this.medeqTableUtil.getSeqNumber("GCNO", this.sEmployeeNo));
        voGCHdr.setGCNO(gcno);
        voTempOrd.setLOTNO(gcno);
      } else {
        voGCHdr.setGCNO(voTempOrd.getLOTNO());
      }

      voGCHdr.setACTIVE("Y");
      voGCHdr.setAMOUNT(voTempOrd.getEXTEND());
      voGCHdr.setBAL(voTempOrd.getEXTEND());
      voGCHdr.setEXPDATE(voTempOrd.getGCEXPDATE());

      TblGCHDR.insert(this.dao, voGCHdr, this.today, this.sEmployeeNo);
    }
  }

  private void saveOrdDtl(OrderDataBean orderDataBean, List<? extends VOORDDTLCommon> vTempOrd, AuditOrder orderAudit)
      throws DAOException, SQLException {

    // NEW AUDIT TRAIL
    // Before deleting anything save the list of order details
    List<VOORDDTL> vOldOrdDtls = orderAudit.getOrderDetails(this.dao);

    // Delete ORDDTL and shift ORDDTL
    StringBuilder sOrdDtlID = new StringBuilder();

    StringBuilder sql = new StringBuilder(
        "SELECT ORDDTLID FROM TEMPORD WITH(NOLOCK) WHERE DELETED = 'Y' AND AUDIT_USER = ")
            .append(this.sEmployeeNo);
    sql.append(
        " AND ORDDTLID NOT IN(SELECT ORDDTLID FROM TEMPORD WITH(NOLOCK) WHERE DELETED <> 'Y' AND AUDIT_USER = ")
        .append(this.sEmployeeNo).append(")");

    RowSet rs = this.dao.Exce_Select(sql);
    Set<Integer> delSet = new HashSet<Integer>();
    while (rs.next()) {
      int aId = rs.getInt("ORDDTLID");
      if (aId > 0) {
        sOrdDtlID.append(sOrdDtlID.length() > 0 ? "," : "").append(aId);
        delSet.add(aId);
      }
    }

    List<String> vCriteria = new ArrayList<String>();
    if (sOrdDtlID.length() > 0) {
      vCriteria.clear();
      vCriteria.add("SYSTEMGENERATEID IN(" + sOrdDtlID + ")");

      if (orderDataBean.getPreAuth().equals("Y")) {
        TblPREORDDTL.delete(this.dao, vCriteria);
      } else {
        TblORDDTL.delete(this.dao, vCriteria);
      }
    }

    sql = new StringBuilder("UPDATE ").append(orderDataBean.getPreAuth().equals("Y") ? "PREORDDTL" : "ORDDTL");
    sql.append(" SET SEQNO = SEQNO + ").append(2 * vTempOrd.size());
    sql.append(" WHERE ORDERNO = ").append(orderDataBean.getOrderNo());
    this.dao.Exce_Update_Del_ins(sql);

    // Iterating TEMPORDs
    for (int i = 0; i < vTempOrd.size(); i++) {
      VOTEMPORD voTempOrd = (VOTEMPORD) vTempOrd.get(i);

      VOORDDTLCommon voOrdDtlCommon;
      if (orderDataBean.getPreAuth().equals("Y")) {
        voOrdDtlCommon = new VOPREORDDTL();

        if (orderDataBean.getEdit().equals("Y")) {
          voOrdDtlCommon = TblPREORDDTL.getVOByIdentity(this.dao, voTempOrd.getORDDTLID());
        }
      } else {
        voOrdDtlCommon = new VOORDDTL();

        if (orderDataBean.getEdit().equals("Y")) {
          voOrdDtlCommon = TblORDDTL.getVOByIdentity(this.dao, voTempOrd.getORDDTLID());
        }

        ((VOORDDTL) voOrdDtlCommon).setBILLCTR(voTempOrd.getBILLCTR() > 1 ? voTempOrd.getBILLCTR() : 1);
        ((VOORDDTL) voOrdDtlCommon)
            .setACTUALBILLCTR(voTempOrd.getACTUALBILLCTR() > 1 ? voTempOrd.getACTUALBILLCTR() : 1);
        ((VOORDDTL) voOrdDtlCommon).setPICKUPNO(voTempOrd.getPICKUPNO());
        ((VOORDDTL) voOrdDtlCommon).setFMSENT(voTempOrd.getFMSENT());
        ((VOORDDTL) voOrdDtlCommon).setQUOTENO(voTempOrd.getQUOTENO());
        ((VOORDDTL) voOrdDtlCommon).setREASON(voTempOrd.getREASON());
      }

      if (StringUtils.isNotBlank(voTempOrd.getMAKE()) && StringUtils.isNotBlank(voTempOrd.getPARTNO())) {
        // Update TITLE.HCPCS
        List<String> vSet = new ArrayList<String>();
        vSet.clear();
        vSet.add("HCPCS = '" + voTempOrd.getHCPCS() + "'");

        vCriteria.clear();
        vCriteria.add("MAKE = '" + voTempOrd.getMAKE() + "'");
        vCriteria.add("PARTNO = '" + voTempOrd.getPARTNO() + "'");
        vCriteria.add("HCPCS = ''");

        TblTITLE.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);
      }

      voOrdDtlCommon.setORDERNO(orderDataBean.getOrderNo());
      voOrdDtlCommon.setSEQNO(i + 1);
      voOrdDtlCommon.setSKU(voTempOrd.getSKU());
      voOrdDtlCommon.setMAKE(voTempOrd.getMAKE());
      voOrdDtlCommon.setPARTNO(voTempOrd.getPARTNO());

      voOrdDtlCommon.setDESCRIPTION(voTempOrd.getDESCRIPTION());
      voOrdDtlCommon.setITEMTYPE(voTempOrd.getITEMTYPE());
      voOrdDtlCommon.setORDQTY(voTempOrd.getORDQTY());
      voOrdDtlCommon.setBACKORDER(voTempOrd.getBACKORDER());
      voOrdDtlCommon.setSHIPQTY(voTempOrd.getSHIPQTY());
      voOrdDtlCommon.setRTNQTY(voTempOrd.getRTNQTY());
      voOrdDtlCommon.setUNITPRICE(voTempOrd.getUNITPRICE());
      voOrdDtlCommon.setSALEPRICE(voTempOrd.getSALEPRICE());
      voOrdDtlCommon.setCOST(voTempOrd.getCOST());
      voOrdDtlCommon.setTAXABLE(voTempOrd.getTAXABLE());
      voOrdDtlCommon.setDAILYRATE(voTempOrd.getDAILYRATE());
      voOrdDtlCommon.setWKRATE(voTempOrd.getWKRATE());
      voOrdDtlCommon.setMONRATE(voTempOrd.getMONRATE());
      voOrdDtlCommon.setMINDAYS(voTempOrd.getMINDAYS());
      voOrdDtlCommon.setR2OAMT(voTempOrd.getR2OAMT());
      voOrdDtlCommon.setR2OTERMS(voTempOrd.getR2OTERMS());
      voOrdDtlCommon.setR3AMT(voTempOrd.getR3AMT());
      voOrdDtlCommon.setEXTEND(voTempOrd.getEXTEND());
      voOrdDtlCommon.setOFFICE(voTempOrd.getOFFICE());

      // Gift certificate
      this.saveGiftCertificate(orderDataBean, voTempOrd);

      voOrdDtlCommon.setLOTNO(voTempOrd.getLOTNO());
      voOrdDtlCommon.setRX(voTempOrd.getRXSEQNO() != 0 ? "Y" : "N");
      voOrdDtlCommon.setACTUALUOM(voTempOrd.getACTUALUOM());
      voOrdDtlCommon.setACTUALQTY(voTempOrd.getACTUALQTY());
      voOrdDtlCommon.setDROPSHIP(voTempOrd.getDROPSHIP());
      voOrdDtlCommon.setVENDOR(voTempOrd.getVENDOR());
      voOrdDtlCommon.setHCPCS(voTempOrd.getHCPCS());

      voOrdDtlCommon.setITEMSOURCE(voTempOrd.getITEMSOURCE());

      voOrdDtlCommon.setRXTABLE(voTempOrd.getRXTABLE());
      voOrdDtlCommon.setRXSEQNO(voTempOrd.getRXSEQNO());
      voOrdDtlCommon.setRXITEMSEQNO(voTempOrd.getRXITEMSEQNO());

      voOrdDtlCommon.setORDERDATE(
          orderDataBean.getEdit().equals("Y") ? voTempOrd.getORDERDATE() : orderDataBean.getOrderDate());
      voOrdDtlCommon.setDELIVERYMETHOD(voTempOrd.getDELIVERYMETHOD());
      voOrdDtlCommon.setCLAIM(voTempOrd.getCLAIM());
      voOrdDtlCommon.setPONO(voTempOrd.getPONO());
      voOrdDtlCommon.setDATEDEL(voTempOrd.getDATEDEL());

      voOrdDtlCommon.setMINDAYS(voTempOrd.getMINDAYS());
      voOrdDtlCommon.setMINAMT(voTempOrd.getMINAMT());

      voOrdDtlCommon.setDIAGPOINTER(voTempOrd.getDIAGPOINTER());
      voOrdDtlCommon.setDIAGPOINTER_ICD10(voTempOrd.getDIAGPOINTER_ICD10());
      voOrdDtlCommon.setCLAIM_LINE_NOTE(voTempOrd.getCLAIM_LINE_NOTE());
      voOrdDtlCommon.setPKID(voTempOrd.getPKID());

      voOrdDtlCommon.setALLOWABLE(voTempOrd.getALLOWABLE());
      voOrdDtlCommon.setPRIMARY(voTempOrd.getPRIMARY());
      voOrdDtlCommon.setSECONDARY(voTempOrd.getSECONDARY());
      voOrdDtlCommon.setTERTIARY(voTempOrd.getTERTIARY());
      voOrdDtlCommon.setPATRESP(voTempOrd.getPATRESP());
      voOrdDtlCommon.setSPECIALORDER(voTempOrd.getSPECIALORDER());
      voOrdDtlCommon.setHARDSHIP(voTempOrd.getHARDSHIP());

      voOrdDtlCommon.setMODIFIER1(voTempOrd.getMODIFIER1());
      voOrdDtlCommon.setMODIFIER2(voTempOrd.getMODIFIER2());
      voOrdDtlCommon.setMODIFIER3(voTempOrd.getMODIFIER3());
      voOrdDtlCommon.setMODIFIER4(voTempOrd.getMODIFIER4());

      voOrdDtlCommon.setSPDAILY(voTempOrd.getSPDAILY());
      voOrdDtlCommon.setAUTHNO(voTempOrd.getAUTHNO());
      voOrdDtlCommon.setPANO(voTempOrd.getPANO());

      voOrdDtlCommon.setCAPPERIOD(voTempOrd.getCAPPERIOD());
      voOrdDtlCommon.setCAPPURCHASE(voTempOrd.getCAPPURCHASE());
      voOrdDtlCommon.setRENT2PURCHASECAP(voTempOrd.isRENT2PURCHASECAP());
      voOrdDtlCommon.setRXNUMBER(voTempOrd.getRXNUMBER());
      voOrdDtlCommon.setPHARMACYID(voTempOrd.getPHARMACYID());

      voOrdDtlCommon.setDISCOUNT_AMOUNT(voTempOrd.getDISCOUNT_AMOUNT());
      voOrdDtlCommon.setDISCOUNT_PERCENTAGE(voTempOrd.getDISCOUNT_PERCENTAGE());
      voOrdDtlCommon.setINSURANCE(voTempOrd.getINSURANCE());
      voOrdDtlCommon.setWKCUTOFF(voTempOrd.getWKCUTOFF());

      voOrdDtlCommon.setWO_AMOUNT(voTempOrd.getWO_AMOUNT());
      voOrdDtlCommon.setWO_MONTH(voTempOrd.getWO_MONTH());
      voOrdDtlCommon.setWO_PERCENTAGE(voTempOrd.getWO_PERCENTAGE());
          voOrdDtlCommon.setADJ_PERCENTAGE(voTempOrd.getADJ_PERCENTAGE());
      voOrdDtlCommon.setOVERRIDE_ALLOWABLE(voTempOrd.getOVERRIDE_ALLOWABLE());
      voOrdDtlCommon.setDEDUCTIBLE(voTempOrd.getDEDUCTIBLE());
      voOrdDtlCommon.setSIDE(voTempOrd.getSIDE());

      voOrdDtlCommon.setTAXID(voTempOrd.getTAXID());

      voOrdDtlCommon.setHOLDDELIVERY(voTempOrd.isHOLDDELIVERY());

      // Insert to ORDDTL table
      if (orderDataBean.getPreAuth().equals("Y")) {
        if (voTempOrd.getORDDTLID() == 0) {
          TblPREORDDTL.insert(this.dao, (VOPREORDDTL) voOrdDtlCommon, this.today, this.sEmployeeNo);
        } else {
          List<String> vTable = new ArrayList<String>();
          vTable.clear();
          vTable.add("PREORDDTL");

          vCriteria.clear();
          vCriteria.add("SYSTEMGENERATEID = " + voTempOrd.getORDDTLID());

          List<String> vSet = new ArrayList<String>();
          vSet.clear();
          vSet.add("SEQNO = " + (i + 1));

          this.dao.update(vTable, vSet, vCriteria);

          TblPREORDDTL.update(this.dao, (VOPREORDDTL) voOrdDtlCommon, this.today, this.sEmployeeNo);
        }
      } else {
        if (voTempOrd.getORDDTLID() == 0) {
          TblORDDTL.insert(this.dao, (VOORDDTL) voOrdDtlCommon, this.today, this.sEmployeeNo);
        } else {
          List<String> vTable = new ArrayList<String>();
          vTable.clear();
          vTable.add("ORDDTL");

          vCriteria.clear();
          vCriteria.add("SYSTEMGENERATEID = " + voTempOrd.getORDDTLID());

          List<String> vSet = new ArrayList<String>();
          vSet.clear();
          vSet.add("SEQNO = " + (i + 1));

          this.dao.update(vTable, vSet, vCriteria);

          TblORDDTL.update(this.dao, (VOORDDTL) voOrdDtlCommon, this.today, this.sEmployeeNo);
        }

        // For PO list
        int iOrdDtlID = TblORDDTL.getVO(this.dao, voOrdDtlCommon.getORDERNO(), voOrdDtlCommon.getSEQNO())
            .getSYSTEMGENERATEID();

        for (int j = 0; j < orderDataBean.getPoLists().size(); j++) {
          List<POSplitDataBean> sublist = orderDataBean.getPoLists().get(j);
          for (POSplitDataBean poSplit : sublist) {
            if (poSplit.getTempOrdID() == voTempOrd.getSYSTEMGENERATEID()) {
              poSplit.getPOSplit().setORDDTLID(iOrdDtlID);
            }
          }
        }
      }

      // NEW AUDIT TRAIL
      // WRITE DIFFERENCE TO AUDIT
      if(null != vOldOrdDtls && vOldOrdDtls.size() > 0) {
        try {
          VOORDDTL oldDetail = vOldOrdDtls.get(i);
          if(oldDetail != null) {
            orderAudit.writeOrderDetailDiff(oldDetail, voOrdDtlCommon, i);
          }
        } catch (ArrayIndexOutOfBoundsException oob) {
          // New orders may have no previous details
        }
      }

      // Reset SYSTEMGENERATEID
      sql = new StringBuilder("SELECT TOP 1 SYSTEMGENERATEID FROM ")
          .append(orderDataBean.getPreAuth().equals("Y") ? "PREORDDTL" : "ORDDTL");
      sql.append(" WITH(NOLOCK) WHERE ORDERNO = ").append(voOrdDtlCommon.getORDERNO()).append(" AND SEQNO = ")
          .append(voOrdDtlCommon.getSEQNO());
      rs = this.dao.Exce_Select(sql);
      if (rs.next()) {
        voOrdDtlCommon.setSYSTEMGENERATEID(rs.getInt("SYSTEMGENERATEID"));
      }

      if (!orderDataBean.getPreAuth().equals("Y") && voTempOrd.getSP_ORDDTLID() < 0) {
        int oldOrdDtlID = voTempOrd.getSP_ORDDTLID() * -1;
        sql = new StringBuilder("UPDATE ORDDTL SET SP_ORDDTLID = ")
            .append(voOrdDtlCommon.getSYSTEMGENERATEID());
        sql.append(" WHERE SYSTEMGENERATEID = ").append(oldOrdDtlID);
        this.dao.Exce_Update_Del_ins(sql);
      }

      this.addToRequestFile(orderDataBean, voOrdDtlCommon);
    }
  }

  private void backupOrdDtl2OrdDtl_Hist(int orderno) throws DAOException, SQLException {
    int iRevNo = 1;
    RowSet rs = this.dao.Exce_Select(
        "SELECT TOP 1 MAX(REVNO) AS REVNO FROM ORDDTL_HIST WITH(NOLOCK) WHERE ORDERNO = " + orderno);
    if (rs.next()) {
      iRevNo = rs.getInt("REVNO") + 1;
    }

    List<String> vCriteria = new ArrayList<String>();
    vCriteria.clear();
    vCriteria.add("ORDERNO = " + orderno);

    List<VOORDDTL> vOrig = TblORDDTL.getVOVector(this.dao, vCriteria);

    for (VOORDDTL voOrdDtl : vOrig) {
      VOORDDTL_HIST voOrdDtlHist = new VOORDDTL_HIST();

      voOrdDtlHist.setSYSTEMGENERATEID(voOrdDtl.getSYSTEMGENERATEID());
      voOrdDtlHist.setORDERNO(voOrdDtl.getORDERNO());
      voOrdDtlHist.setREVNO(iRevNo);
      voOrdDtlHist.setSEQNO(voOrdDtl.getSEQNO());
      voOrdDtlHist.setSKU(voOrdDtl.getSKU());
      voOrdDtlHist.setOFFICE(voOrdDtl.getOFFICE());
      voOrdDtlHist.setLOTNO(voOrdDtl.getLOTNO());
      voOrdDtlHist.setMAKE(voOrdDtl.getMAKE());
      voOrdDtlHist.setPARTNO(voOrdDtl.getPARTNO());
      voOrdDtlHist.setDESCRIPTION(voOrdDtl.getDESCRIPTION());
      voOrdDtlHist.setITEMTYPE(voOrdDtl.getITEMTYPE());
      voOrdDtlHist.setORDQTY(voOrdDtl.getORDQTY());
      voOrdDtlHist.setBACKORDER(voOrdDtl.getBACKORDER());
      voOrdDtlHist.setSHIPQTY(voOrdDtl.getSHIPQTY());
      voOrdDtlHist.setRTNQTY(voOrdDtl.getRTNQTY());
      voOrdDtlHist.setUNITPRICE(voOrdDtl.getUNITPRICE());
      voOrdDtlHist.setSALEPRICE(voOrdDtl.getSALEPRICE());
      voOrdDtlHist.setCOST(voOrdDtl.getCOST());
      voOrdDtlHist.setTAXABLE(voOrdDtl.getTAXABLE());
      voOrdDtlHist.setDAILYRATE(voOrdDtl.getDAILYRATE());
      voOrdDtlHist.setWKRATE(voOrdDtl.getWKRATE());
      voOrdDtlHist.setMONRATE(voOrdDtl.getMONRATE());
      voOrdDtlHist.setMINDAYS(voOrdDtl.getMINDAYS());
      voOrdDtlHist.setR2OAMT(voOrdDtl.getR2OAMT());
      voOrdDtlHist.setR2OTERMS(voOrdDtl.getR2OTERMS());
      voOrdDtlHist.setR3AMT(voOrdDtl.getR3AMT());
      voOrdDtlHist.setDATEDEL(voOrdDtl.getDATEDEL());
      voOrdDtlHist.setDELHOURS(voOrdDtl.getDELHOURS());
      voOrdDtlHist.setDATERTN(voOrdDtl.getDATERTN());
      voOrdDtlHist.setRTNHOURS(voOrdDtl.getRTNHOURS());
      voOrdDtlHist.setEXTEND(voOrdDtl.getEXTEND());
      voOrdDtlHist.setRX(voOrdDtl.getRX());
      voOrdDtlHist.setACTUALUOM(voOrdDtl.getACTUALUOM());
      voOrdDtlHist.setACTUALQTY(voOrdDtl.getACTUALQTY());
      voOrdDtlHist.setVENDOR(voOrdDtl.getVENDOR());
      voOrdDtlHist.setITEMSOURCE(voOrdDtl.getITEMSOURCE());
      voOrdDtlHist.setHCPCS(voOrdDtl.getHCPCS());
      voOrdDtlHist.setRXTABLE(voOrdDtl.getRXTABLE());
      voOrdDtlHist.setRXSEQNO(voOrdDtl.getRXSEQNO());
      voOrdDtlHist.setRXITEMSEQNO(voOrdDtl.getRXITEMSEQNO());
      voOrdDtlHist.setORDERDATE(voOrdDtl.getORDERDATE());
      voOrdDtlHist.setDELIVERYMETHOD(voOrdDtl.getDELIVERYMETHOD());
      voOrdDtlHist.setCLAIM(voOrdDtl.getCLAIM());
      voOrdDtlHist.setDROPSHIP(voOrdDtl.getDROPSHIP());
      voOrdDtlHist.setPONO(voOrdDtl.getPONO());
      voOrdDtlHist.setBILLCTR(voOrdDtl.getBILLCTR());
      voOrdDtlHist.setACTUALBILLCTR(voOrdDtl.getACTUALBILLCTR());
      voOrdDtlHist.setPICKUPNO(voOrdDtl.getPICKUPNO());
      voOrdDtlHist.setMINAMT(voOrdDtl.getMINAMT());
      voOrdDtlHist.setVNDORDERNO(voOrdDtl.getVNDORDERNO());
      voOrdDtlHist.setDIAGPOINTER(voOrdDtl.getDIAGPOINTER());
      voOrdDtlHist.setDIAGPOINTER_ICD10(voOrdDtl.getDIAGPOINTER_ICD10());
      voOrdDtlHist.setCLAIM_LINE_NOTE(voOrdDtl.getCLAIM_LINE_NOTE());
      voOrdDtlHist.setPKID(voOrdDtl.getPKID());
      voOrdDtlHist.setALLOWABLE(voOrdDtl.getALLOWABLE());
      voOrdDtlHist.setPRIMARY(voOrdDtl.getPRIMARY());
      voOrdDtlHist.setSECONDARY(voOrdDtl.getSECONDARY());
      voOrdDtlHist.setTERTIARY(voOrdDtl.getTERTIARY());
      voOrdDtlHist.setPATRESP(voOrdDtl.getPATRESP());
      voOrdDtlHist.setSPECIALORDER(voOrdDtl.getSPECIALORDER());
      voOrdDtlHist.setHARDSHIP(voOrdDtl.getHARDSHIP());

      voOrdDtlHist.setMODIFIER1(voOrdDtl.getMODIFIER1());
      voOrdDtlHist.setMODIFIER2(voOrdDtl.getMODIFIER2());
      voOrdDtlHist.setMODIFIER3(voOrdDtl.getMODIFIER3());
      voOrdDtlHist.setMODIFIER4(voOrdDtl.getMODIFIER4());

      voOrdDtlHist.setAUTHNO(voOrdDtl.getAUTHNO());
      voOrdDtlHist.setPANO(voOrdDtl.getPANO());
      voOrdDtlHist.setSPDAILY(voOrdDtl.getSPDAILY());

      voOrdDtlHist.setSP_ORDDTLID(voOrdDtl.getSP_ORDDTLID());

      voOrdDtlHist.setCAPPERIOD(voOrdDtl.getCAPPERIOD());
      voOrdDtlHist.setCAPPURCHASE(voOrdDtl.getCAPPURCHASE());
      voOrdDtlHist.setRENT2PURCHASECAP(voOrdDtl.isRENT2PURCHASECAP());
      voOrdDtlHist.setFMSENT(voOrdDtl.getFMSENT());
      voOrdDtlHist.setRXNUMBER(voOrdDtl.getRXNUMBER());
      voOrdDtlHist.setPHARMACYID(voOrdDtl.getPHARMACYID());

      voOrdDtlHist.setDISCOUNT_AMOUNT(voOrdDtl.getDISCOUNT_AMOUNT());
      voOrdDtlHist.setDISCOUNT_PERCENTAGE(voOrdDtl.getDISCOUNT_PERCENTAGE());
      voOrdDtlHist.setINSURANCE(voOrdDtl.getINSURANCE());

      voOrdDtlHist.setWKCUTOFF(voOrdDtl.getWKCUTOFF());
      voOrdDtlHist.setWO_MONTH(voOrdDtl.getWO_MONTH());
      voOrdDtlHist.setWO_AMOUNT(voOrdDtl.getWO_AMOUNT());
      voOrdDtlHist.setWO_PERCENTAGE(voOrdDtl.getWO_PERCENTAGE());
      voOrdDtlHist.setOVERRIDE_ALLOWABLE(voOrdDtl.getOVERRIDE_ALLOWABLE());
      voOrdDtlHist.setDEDUCTIBLE(voOrdDtl.getDEDUCTIBLE());
      voOrdDtlHist.setSIDE(voOrdDtl.getSIDE());
      voOrdDtlHist.setTAXID(voOrdDtl.getTAXID());
      voOrdDtlHist.setREASON(voOrdDtl.getREASON());

      TblORDDTL_HIST.insert(this.dao, voOrdDtlHist, this.today, this.sEmployeeNo);
    }
  }

  private void updatePreAuthStatus(OrderDataBean orderDataBean) throws DAOException, SQLException {
    if (!orderDataBean.getPreAuth().equals("Y")) {
      for (String s : orderDataBean.getPreAuthNos()) {
        int preauthOrderNo = Integer.parseInt(s);
        VOPREORDHDR x = TblPREORDHDR.getVO(this.dao, preauthOrderNo);
        x.setORDSTATUS("C");
        x.setREALORDERNO(orderDataBean.getOrderNo());
        TblPREORDHDR.update(this.dao, x, this.today, this.sEmployeeNo);
      }
    }
  }

  private void clean(int iOrderNo, int iTempOrderNo, String sCustomerID) throws DAOException, SQLException {
    StringBuilder sSql = new StringBuilder("UPDATE NOTESDTL SET NOTESKEY2 = ").append(iOrderNo)
        .append(", NOTESSTATUS = 'P' WHERE NOTESKEY2 = ").append(iTempOrderNo);
    sSql.append(" AND NOTESSTATUS = 'T' AND NOTESTYPE = 'O'");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder("UPDATE ENTERAL_RXHDR SET ORDERNO = ").append(iOrderNo)
        .append(", RXSTATUS = 'P' WHERE ORDERNO = ").append(iTempOrderNo).append(" AND RXSTATUS='T'");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder("UPDATE DIABETIC_RXHDR SET ORDERNO = ").append(iOrderNo)
        .append(", RXSTATUS = 'P' WHERE ORDERNO = ").append(iTempOrderNo).append(" AND RXSTATUS='T'");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder("UPDATE GENERIC_RXHDR SET ORDERNO = ").append(iOrderNo)
        .append(", RXSTATUS = 'P' WHERE ORDERNO = ").append(iTempOrderNo);
    sSql.append(" AND RXSTATUS = 'T'");
    this.dao.Exce_Update_Del_ins(sSql);
  }

  public void deleteClean(int iOrderNo, int iTempOrderNo, String sCustomerID) throws DAOException, SQLException {
    StringBuilder sSql = new StringBuilder(
        "DELETE FROM NOTESNOTIFYDTL WHERE NOTIFYID IN (SELECT A.NOTIFYID FROM NOTESNOTIFYHDR A, NOTESDTL B WHERE ");
    sSql.append("A.NOTESID = B.NOTESID AND B.REMIND = '1' AND B.NOTESKEY2 = ").append(iTempOrderNo)
        .append(" AND B.NOTESSTATUS = 'T' AND B.NOTESTYPE = 'O')");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder(
        "DELETE FROM NOTESMAILNOTIFYDTL WHERE NOTIFYID IN (SELECT A.NOTIFYID FROM NOTESNOTIFYHDR A, NOTESDTL B WHERE ");
    sSql.append("A.NOTESID = B.NOTESID AND B.REMIND = '1' AND B.NOTESKEY2 = ").append(iTempOrderNo)
        .append(" AND B.NOTESSTATUS = 'T' AND B.NOTESTYPE = 'O')");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder(
        "DELETE FROM NOTESPOPUPNOTIFYDTL WHERE NOTIFYID IN (SELECT A.NOTIFYID FROM NOTESNOTIFYHDR A, NOTESDTL B WHERE ");
    sSql.append("A.NOTESID = B.NOTESID AND B.REMIND = '1' AND B.NOTESKEY2 = ").append(iTempOrderNo)
        .append(" AND B.NOTESSTATUS = 'T' AND B.NOTESTYPE = 'O')");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder(
        "DELETE FROM NOTESNOTIFYHDR WHERE NOTIFYID IN (SELECT A.NOTIFYID FROM NOTESNOTIFYHDR A, NOTESDTL B WHERE ");
    sSql.append("A.NOTESID = B.NOTESID AND B.REMIND = '1' AND B.NOTESKEY2 = ").append(iTempOrderNo)
        .append(" AND B.NOTESSTATUS = 'T' AND B.NOTESTYPE = 'O')");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder("DELETE FROM NOTESDTL WHERE NOTESKEY2 = ").append(iTempOrderNo)
        .append(" AND NOTESSTATUS = 'T' AND NOTESTYPE = 'O'");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder(
        "DELETE FROM ENTERAL_RXDTL WHERE RXSEQNO IN (SELECT RXSEQNO FROM ENTERAL_RXHDR WHERE RXSTATUS = 'T' AND ORDERNO = ")
            .append(iTempOrderNo).append(")");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder("DELETE FROM ENTERAL_RXHDR WHERE RXSTATUS = 'T' AND ORDERNO = ").append(iTempOrderNo);
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder(
        "DELETE FROM DIABETIC_RXDTL WHERE RXSEQNO IN (SELECT RXSEQNO FROM DIABETIC_RXHDR WHERE RXSTATUS = 'T' AND ORDERNO = ")
            .append(iTempOrderNo).append(")");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder("DELETE FROM DIABETIC_RXHDR WHERE RXSTATUS = 'T' AND ORDERNO = ").append(iTempOrderNo);
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder(
        "DELETE FROM GENERIC_RXDTL WHERE RXSEQNO IN (SELECT RXSEQNO FROM GENERIC_RXHDR WHERE RXSTATUS = 'T' AND ORDERNO = ")
            .append(iTempOrderNo).append(")");
    this.dao.Exce_Update_Del_ins(sSql);

    sSql = new StringBuilder("DELETE FROM GENERIC_RXHDR WHERE RXSTATUS = 'T' AND ORDERNO = ").append(iTempOrderNo);
    this.dao.Exce_Update_Del_ins(sSql);
  }

  // Method used to set VOTEMPORD
  public VOTEMPORD setTempOrd(OrderDataBean orderDataBean, String sOfficeNo, String sMake, String sPartNo,
      String sDescription, PriceUtil price, String sCustomerID, String sSellUOM, String sDropShip,
      String sCusType, int iActualQty, int iOrderQty, int iBackOrderQty, String sSKU, String sLotNo)
      throws Exception {
    return this.setTempOrd(orderDataBean, sOfficeNo, sMake, sPartNo, sDescription, price, sCustomerID, sSellUOM,
        sDropShip, sCusType, iActualQty, iOrderQty, iBackOrderQty, sSKU, sLotNo, 0);
  }

  public VOTEMPORD setTempOrd(OrderDataBean orderDataBean, String sOfficeNo, String sMake, String sPartNo,
      String sDescription, PriceUtil price, String sCustomerID, String sSellUOM, String sDropShip,
      String sCusType, int iActualQty, int iOrderQty, int iBackOrderQty, String sSKU, String sLotNo, int iPKID)
      throws Exception {
    DefaultUtil defaultUtil = new DefaultUtil(this.hs, this.dao);

    VOTEMPORD voTempOrd = new VOTEMPORD();
    voTempOrd.setPREAUTH(orderDataBean.getPreAuth().equals("Y") ? "Y" : "N");
    voTempOrd.setORDERNO(orderDataBean.getTempOrderNo());

    orderDataBean.setTempOrdSeqNo(this.dao);
    voTempOrd.setSEQNO(orderDataBean.getTempSeqNo());
    orderDataBean.setTempOrdSeqNo(this.dao);

    String sInvtorySKU = "";

    StringBuilder sql = new StringBuilder("SELECT TOP 1 SKU FROM INVTORY WITH(NOLOCK) WHERE");
    sql.append(" MAKE='").append(sMake).append("'");
    sql.append(" AND PARTNO = '").append(sPartNo).append("'");
    sql.append(" AND OFFICE = '").append(sOfficeNo).append("'");
    sql.append(" AND SELLUOM = '").append(sSellUOM).append("'");
    sql.append(" AND INVTYPE = 'SINV'");
    sql.append(" ORDER BY ONHANDQTY DESC");

    RowSet rs = this.dao.Exce_Select(sql);
    if (rs.next()) {
      sInvtorySKU = StringUtil.validate(rs.getString("SKU"));
    }

    if (orderDataBean.getPOS().equals("Y")) {
      if (iPKID > 0) {
        StringBuilder sqlSku = new StringBuilder("SELECT SKU FROM INVTORY WITH(NOLOCK) WHERE MAKE='")
            .append(sMake).append("' AND PARTNO = '").append(sPartNo).append("'").append(" AND OFFICE='")
            .append(sOfficeNo).append("' AND SELLUOM = '").append(sSellUOM).append("' AND ONHANDQTY > 0");

        if (sCusType.startsWith("R")) {
          sqlSku.append(" AND INVTYPE = 'RINV'");
        }
        RowSet rsSku = dao.Exce_Select(sqlSku);
        rsSku.last();
        if (rsSku.getRow() == 1) {
          voTempOrd.setSKU(StringUtil.validate(rsSku.getString("SKU")));
        } else {
          voTempOrd.setSKU("NA");
        }
      } else if (StringUtils.isBlank(sSKU)) {
        voTempOrd.setSKU(sInvtorySKU.length() > 0 ? sInvtorySKU : "NA");
      } else {
        voTempOrd.setSKU(sSKU);
      }
    } else if (defaultUtil.getParameter("OSHOWSKU").equals("Y") && !orderDataBean.getPreAuth().equals("Y")
        && StringUtils.isNotBlank(sSKU)) {
      voTempOrd.setSKU(sSKU);
    } else {
      voTempOrd.setSKU("NA");
    }

    voTempOrd.setOFFICE(sOfficeNo);

    if (StringUtils.isNotBlank(voTempOrd.getSKU()) && StringUtils.isBlank(sLotNo)) {
      sql = new StringBuilder("SELECT TOP 1 LOTNO FROM INVTORY WITH(NOLOCK)");
      sql.append(" WHERE SKU = '").append(voTempOrd.getSKU()).append("'");
      sql.append(" AND OFFICE = '").append(voTempOrd.getOFFICE()).append("'");
      sql.append(" AND ONHANDQTY > 0");
      rs = this.dao.Exce_Select(sql);
      if (rs.next()) {
        sLotNo = StringUtil.validate(rs.getString("LOTNO"));
      }
    }

    voTempOrd.setLOTNO(sLotNo);

    voTempOrd.setMAKE(sMake);
    voTempOrd.setPARTNO(sPartNo);
    voTempOrd.setDESCRIPTION(sDescription);
    voTempOrd.setORDERDATE(orderDataBean.getOrderDate());
    voTempOrd.setSHIPQTY(0);
    voTempOrd.setRTNQTY(0);
    voTempOrd.setPKID(iPKID);

    this.setPriceToTempOrd(orderDataBean, voTempOrd, price, false);

    voTempOrd.setR2OAMT(0);
    voTempOrd.setR2OTERMS(0);
    voTempOrd.setR3AMT(0);

    if (orderDataBean.getPreAuth().equals("Y")) {
      voTempOrd.setORDSTATUS("P");
    } else {
      voTempOrd.setORDSTATUS("");
    }

    voTempOrd.setCUSTOMERID(sCustomerID);
    voTempOrd.setAUDIT_USER(this.sEmployeeNo);
    voTempOrd.setAUDIT_DATE(this.today.getDate());
    voTempOrd.setAUDIT_TIME(this.today.getTime());
    voTempOrd.setRXSEQNO(0);
    voTempOrd.setRXITEMSEQNO(0);
    voTempOrd.setACTUALQTY(iActualQty);
    voTempOrd.setACTUALUOM(sSellUOM);
    voTempOrd.setDROPSHIP(sDropShip.trim().equals("Y") ? "Y" : "N");

    voTempOrd.setBACKORDER(iBackOrderQty);
    voTempOrd.setEXTEND(iActualQty * voTempOrd.getUNITPRICE());
    voTempOrd.setORDQTY(iOrderQty);
    voTempOrd.setITEMTYPE(sCusType.startsWith("R") ? "REN1" : "SALE");

    voTempOrd.setITEMSOURCE("");
    voTempOrd.setINSURANCE(orderDataBean.getCurrentInsurance());

    // Case: 34659
    voTempOrd.setWKCUTOFF("N");
    rs = dao.Exce_Select(
        "SELECT PRORATE_FST_MONTH FROM CUSTOMER WITH(NOLOCK) WHERE CUSTOMERID = '" + sCustomerID + "'");
    if (rs.next() && StringUtil.validate(rs.getString("PRORATE_FST_MONTH")).equals("Y")) {
      voTempOrd.setWKCUTOFF("Y");
    }

    voTempOrd.setCLAIM(ItemUtil.getClaim(this.dao, sMake, sPartNo));
    if (!orderDataBean.isClaim()
        && (this.dao.getCompanyNo().equals("DCA") || this.dao.getCompanyNo().equals("DCATRAIN"))) {
      voTempOrd.setCLAIM("N");
    }

    if (this.sCompanyNo.startsWith("HOMEMEDICAL")) {
      if ("E0431".equals(voTempOrd.getHCPCS()) && iPKID == 33) {
        voTempOrd.setCLAIM("N");
        voTempOrd.setUNITPRICE(0.00);
        voTempOrd.setSALEPRICE(0.00);
        voTempOrd.setDAILYRATE(0.00);
        voTempOrd.setWKRATE(0.00);
        voTempOrd.setMONRATE(0.00);
        voTempOrd.setEXTEND(0.00);
      }

      if ("E0570".equals(voTempOrd.getHCPCS()) && iPKID == 39) {
        voTempOrd.setCLAIM("N");
      }

      if (iPKID == 173) {
        voTempOrd.setCLAIM("N");
      }
    }

    voTempOrd.setBILLCTR(1);
    voTempOrd.setACTUALBILLCTR(1);

    voTempOrd.setDELIVERYMETHOD("C");

    this.assignDiagnosis(orderDataBean, voTempOrd);

    /*
     * For modifiers
     */
    if (orderDataBean.isClaim()) {
      ModifierUtil.initialize(this.dao, voTempOrd.getHCPCS(), voTempOrd,
          voTempOrd.getITEMTYPE().startsWith("R") ? true : false,
          InsuranceUtil.isMedicareLogic(this.dao, orderDataBean.getInsurance()), orderDataBean.getOrderDate(),
          InsuranceUtil.getAllowableInsurance(this.dao, orderDataBean.getInsurance()), voTempOrd.getSKU(),
          voTempOrd.getOFFICE(), voTempOrd.getLOTNO(), voTempOrd.getMAKE(), voTempOrd.getPARTNO(),
          orderDataBean.getFacilityZip(), voTempOrd.getRXTABLE(), false, orderDataBean.getFillOrderNo(),
          orderDataBean.getInsurance());
      //TTM-424
          ModifierUtil.setCMN4843_HCPCS_MedSouth(dao, voTempOrd.getHCPCS(), orderDataBean.getLiterFlow(),
                 orderDataBean.getLiterFlowType(), voTempOrd);

      ModifierUtil.appendKX4MedSouth(dao, voTempOrd.getHCPCS(), orderDataBean.getOrderDate(),
          orderDataBean.getInsurance(), voTempOrd);
    }

    if (StringUtils.isNotBlank(voTempOrd.getHCPCS()) && StringUtils.isNotBlank(orderDataBean.getOrderDate())) {
      sql = new StringBuilder("SELECT TOP 1 CODE FROM MEDEQCONTROL.DBO.HCPCS WITH(NOLOCK)");
      sql.append(" WHERE XREF1 IN (SELECT CODE FROM MEDEQCONTROL.DBO.HCPCS WITH(NOLOCK))");
      sql.append(" AND TERMDATE > '").append(orderDataBean.getOrderDate()).append("'");
      sql.append(" AND CODEADDDATE < 'orderDataBean.getOrderDate()'");
      sql.append(" AND XREF1 = '").append(voTempOrd.getHCPCS()).append("'");
      rs = this.dao.Exce_Select(sql);
      if (rs.next()) {
        voTempOrd.setHCPCS(StringUtil.validate(rs.getString("CODE")));
      }
    }

    // For allowable
    this.setAllowable(orderDataBean, voTempOrd);

    setHardship(orderDataBean, voTempOrd);

    this.updatePatientResponsibility(orderDataBean, voTempOrd);

    voTempOrd.setCLAIM_LINE_NOTE(
        OrderUtil.getUpn(dao, orderDataBean.getInsurance(), voTempOrd.getMAKE(), voTempOrd.getPARTNO()));

    setCap(orderDataBean, voTempOrd);

    if (this.sCompanyNo.startsWith("JIMM")) {
      voTempOrd.setTAXID(2);
    }

    if ((voTempOrd.getITEMTYPE().startsWith("S") || voTempOrd.getITEMTYPE().startsWith("R"))
        && voTempOrd.getUNITPRICE() == 0) {
      voTempOrd.setALLOWABLE(0);
      voTempOrd.setPATRESP(0);
    }
    return voTempOrd;
  }

  public void setCap(OrderDataBean orderDataBean, VOTEMPORD voTempOrd) throws DAOException, SQLException {
    RowSet rs;
    if ("Y".equals(voTempOrd.getCLAIM()) && "Y".equals(orderDataBean.getFileClaim())) {
      String hcpcs = StringUtil.validate(voTempOrd.getHCPCS());
      if (isNotBlank(hcpcs)) {
        VOHCPCS_SPAN voHcpcsSpan = TblHCPCS_SPAN.getVO(dao, hcpcs);
        String insurance = voTempOrd.getINSURANCE();
        if (isBlank(insurance)) {
          insurance = orderDataBean.getInsurance();
        }

        if ("CR".equals(voHcpcsSpan.getCATEGORY()) && isNotBlank(insurance)) {
          Formatter f = new Formatter();
          f.format("SELECT CAPMONTH FROM INSURANCE WITH(NOLOCK)WHERE CAPMONTH>0 AND INSURANCE='%s'",
              insurance);
          rs = dao.Exce_Select(f.toString());

          if (rs.next()) {
            int capMonth = rs.getInt("CAPMONTH");
            if (capMonth > 0) {
              voTempOrd.setCAPPERIOD(capMonth);
              voTempOrd.setCAPPURCHASE("Y");
            }
          }

          f = new Formatter();
          f.format("SELECT CAPPERIOD,CAPPURCHASE FROM INSURANCE_RENTAL2PURCHASE WITH(NOLOCK)");
          f.format(" WHERE INSURANCE='%s' AND HCPCS='%s'", insurance, hcpcs);
          rs = dao.Exce_Select(f.toString());
          if (rs.next()) {
            voTempOrd.setCAPPERIOD(rs.getInt("CAPPERIOD"));
            voTempOrd.setCAPPURCHASE("Y");
            voTempOrd.setRENT2PURCHASECAP(rs.getBoolean("CAPPURCHASE"));
          }
        }
      }
    }
  }

  private void setHardship(OrderDataBean orderDataBean, VOTEMPORD voTempOrd) throws DAOException, SQLException {
    boolean isHardShip = false;
    if (!orderDataBean.getCustomerID().equals("ZZPOSZZ")) {
      Formatter f = new Formatter();
      f.format("SELECT TOP 1 HARDSHIP FROM CUSTOMER WITH(NOLOCK) WHERE HARDSHIP = 'Y' AND CUSTOMERID = '%s'",
          orderDataBean.getCustomerID());
      RowSet rs = this.dao.Exce_Select(f.toString());
      if (rs.next()) {
        isHardShip = true;
      }
    }

    voTempOrd.setHARDSHIP(isHardShip ? "Y" : "N");
  }

  public void setAllowable(OrderDataBean orderDataBean, VOTEMPORD voTempOrd)
      throws DAOException, SQLException, NumberFormatException, JustAWarning {
    if (orderDataBean.getFileClaim().equalsIgnoreCase("Y")) {
      StringBuilder sql = null;
      RowSet rs = null;

      String state = "";
      String zip = "";
      String allowableInsurance = StringUtils.isNotBlank(orderDataBean.getCurrentInsurance())
          ? orderDataBean.getCurrentInsurance() : orderDataBean.getInsurance();
      if (orderDataBean.isPDP() && orderDataBean.isPOS() && !orderDataBean.isCashCustomer()) {
        if (allowableInsurance.length() == 0) {
          sql = new StringBuilder(
              "SELECT TOP 1 INSURANCE FROM INSURANCE WITH(NOLOCK) WHERE INSURANCETYPE='MEDICARE'");
          rs = dao.Exce_Select(sql);
          if (rs.next()) {
            orderDataBean.setInsurance(StringUtil.validate(rs.getString("INSURANCE")));
          }
        }

        sql = new StringBuilder("SELECT STATE, ZIP FROM OFFICE WITH(NOLOCK) WHERE OFFICE = '").append(sOfficeNo)
            .append("'");
      } else {
        sql = new StringBuilder("SELECT STATE, ZIP FROM CUSTOMER WITH(NOLOCK) WHERE CUSTOMERID = '")
            .append(orderDataBean.getCustomerID()).append("'");
      }

      rs = this.dao.Exce_Select(sql);
      if (rs.next()) {
        state = StringUtil.validate(rs.getString("STATE"));
        zip = StringUtil.validate(rs.getString("ZIP"));
      }

      rs = dao.Exce_Select("SELECT STATE, ZIP FROM GUARANTOR WITH(NOLOCK) WHERE CUSTOMERID = '"
          + orderDataBean.getCustomerID() + "' AND  INSURANCE = '" + orderDataBean.getInsurance() + "'");
      if (rs.next()) {
        state = StringUtils.trimToEmpty(rs.getString("STATE"));
        zip = StringUtils.trimToEmpty(rs.getString("ZIP"));
      }

      double d = AllowableUtil.getAllowable(this.dao, orderDataBean.getHighEnd().equalsIgnoreCase("Y"),
          allowableInsurance, state, zip, voTempOrd.getHCPCS(), voTempOrd.getMODIFIER1(),
          voTempOrd.getMODIFIER2(), voTempOrd.getITEMTYPE(), voTempOrd.getBILLCTR(), voTempOrd.getUNITPRICE(),
          voTempOrd.getCOST(), voTempOrd.getOFFICE(), voTempOrd.getSKU(), voTempOrd.getMAKE(),
          voTempOrd.getPARTNO(), voTempOrd.getACTUALUOM(), voTempOrd.getACTUALQTY(), false,
          Integer.parseInt(orderDataBean.getOrderDate().substring(0, 4)),
          Integer.parseInt(orderDataBean.getOrderDate().substring(4, 6)), orderDataBean.getFillOrderNo(),
          orderDataBean.getOrderDate(), "", orderDataBean.getLiterFlow());

      voTempOrd.setALLOWABLE(d);
    } else {
      voTempOrd.setALLOWABLE(0);
    }
  }

  public void updatePatientResponsibility(OrderDataBean orderDataBean, VOTEMPORD voTempOrd) throws Exception {
    // Case: 28260
    if (voTempOrd.getRXNUMBER().length() > 0 && this.dao.getCompanyNo().startsWith("DCA")) {
      return;
    }

    if (orderDataBean.getFileClaim().equals("Y") && voTempOrd.getCLAIM().equals("Y")) {
      if (orderDataBean.getAcceptAssign().equals("Y")) {
        // Update patient responsibility
        if (StringUtils.isNotBlank(orderDataBean.getInsurance())) {
          double d = voTempOrd.getALLOWABLE();

          double percentage = 0;
          String sql = "SELECT TOP 1 INSURANCEPAYSPERCENTAGE FROM GUARANTOR WITH(NOLOCK) WHERE CUSTOMERID = '"
              + orderDataBean.getCustomerID() + "' AND INSURANCE = '" + orderDataBean.getInsurance()
              + "'";
          RowSet rs = this.dao.Exce_Select(sql);
          if (rs.next()) {
            percentage = rs.getDouble("INSURANCEPAYSPERCENTAGE");
          }

          if (percentage == 0) {
            sql = "SELECT TOP 1 PERCENTAGE FROM INSURANCE WITH(NOLOCK) WHERE INSURANCE = '"
                + orderDataBean.getInsurance() + "'";
            rs = this.dao.Exce_Select(sql);
            if (rs.next()) {
              percentage = rs.getDouble("PERCENTAGE");
            }
          }

          voTempOrd.setPRIMARY(d * percentage * 0.01);

          if (StringUtils.isNotBlank(orderDataBean.getInsurance2())) {
            d = d - voTempOrd.getPRIMARY();
            voTempOrd.setSECONDARY(d);
            voTempOrd.setPATRESP(0);
          } else {
            voTempOrd.setPATRESP(d - voTempOrd.getPRIMARY());
          }
        }
      } else {
        boolean bPOS = OrderDataBean.isPOS(this.dao, orderDataBean);

        TaxComponentBean tc = TaxUtil.getTaxInfo(dao, bPOS, orderDataBean.isBillToTaxable(),
            orderDataBean.getFileClaim().equals("Y") ? true : false,
            TaxUtil.isInsuranceTaxable(dao, orderDataBean.getInsurance()),
            voTempOrd.getTAXABLE().equals("Y") ? true : false,
            voTempOrd.getCLAIM().equals("Y") ? true : false,
            orderDataBean.getAcceptAssign().equals("Y") ? false : true,
            InsuranceUtil.isNonAATaxableOnly(dao, orderDataBean.getInsurance()),
            orderDataBean.getDistrictTaxID(), voTempOrd.getUNITPRICE(), voTempOrd.getACTUALQTY(),
            orderDataBean.getCustomerID(), new DateTimeUtil().getDate(),
            orderDataBean.getVoBillToCustomer().getADDRESS(),
            orderDataBean.getVoBillToCustomer().getADDRESS2(), orderDataBean.getShipToCity(),
            orderDataBean.getShipToCounty(), orderDataBean.getShipToState(), orderDataBean.getShipToZip(),
            orderDataBean.getOfficeNo(), voTempOrd.getOFFICE(), voTempOrd.getSKU(), voTempOrd.getLOTNO(),
            voTempOrd.getMAKE(), voTempOrd.getPARTNO(), voTempOrd.getDESCRIPTION(),
            voTempOrd.getITEMTYPE());
        voTempOrd.setPATRESP(ArithmeticUtil.add(voTempOrd.getEXTEND(), tc.getTaxAmount()));
      }

      if (InsuranceUtil.isMedicaid(this.dao, orderDataBean.getInsurance())
          || InsuranceUtil.isMedicaid(this.dao, orderDataBean.getInsurance2())
          || voTempOrd.getHARDSHIP().equals("Y")) {
        voTempOrd.setPATRESP(0);
      }
    } else {
      boolean bPOS = OrderDataBean.isPOS(this.dao, orderDataBean);

      TaxComponentBean tc = TaxUtil.getTaxInfo(dao, bPOS, orderDataBean.isBillToTaxable(),
          orderDataBean.getFileClaim().equals("Y") ? true : false,
          TaxUtil.isInsuranceTaxable(dao, orderDataBean.getInsurance()),
          voTempOrd.getTAXABLE().equals("Y") ? true : false, voTempOrd.getCLAIM().equals("Y") ? true : false,
          orderDataBean.getAcceptAssign().equals("Y") ? false : true,
          InsuranceUtil.isNonAATaxableOnly(dao, orderDataBean.getInsurance()),
          orderDataBean.getDistrictTaxID(), voTempOrd.getUNITPRICE(), voTempOrd.getACTUALQTY(),
          orderDataBean.getCustomerID(), new DateTimeUtil().getDate(),
          orderDataBean.getVoBillToCustomer().getADDRESS(), orderDataBean.getVoBillToCustomer().getADDRESS2(),
          orderDataBean.getShipToCity(), orderDataBean.getShipToCounty(), orderDataBean.getShipToState(),
          orderDataBean.getShipToZip(), orderDataBean.getOfficeNo(), voTempOrd.getOFFICE(),
          voTempOrd.getSKU(), voTempOrd.getLOTNO(), voTempOrd.getMAKE(), voTempOrd.getPARTNO(),
          voTempOrd.getDESCRIPTION(), voTempOrd.getITEMTYPE());
      voTempOrd.setPATRESP(ArithmeticUtil.add(voTempOrd.getEXTEND(), tc.getTaxAmount()));
    }
  }

  public void setPriceToTempOrd(OrderDataBean orderDataBean, VOTEMPORD voTempOrd, PriceUtil price,
      boolean useOriginalTaxable) throws DAOException, SQLException {
    if (price != null) {
      voTempOrd.setUNITPRICE(price.getUnitPrice());
      voTempOrd.setSALEPRICE(price.getSalePrice());
      voTempOrd.setCOST(price.getCost());

      if (!useOriginalTaxable) {
        voTempOrd.setTAXABLE(price.getTaxable());
      }
      voTempOrd.setDAILYRATE(price.getDailyRate());
      voTempOrd.setWKRATE(price.getWkRate());
      voTempOrd.setMONRATE(price.getMonRate());
      voTempOrd.setMINDAYS(price.getMinDays());
      voTempOrd.setMINAMT(price.getMinAmt());
      voTempOrd.setSPDAILY(price.getSpDaily());

      if (StringUtils.isBlank(voTempOrd.getHCPCS())) {
        voTempOrd.setHCPCS(price.getHcpcs());
      }

      if (voTempOrd.getPKID() > 0) {
        StringBuilder sql = new StringBuilder(
            "SELECT TOP 1 PKHDR.HCPCS AS X FROM PKHDR WITH(NOLOCK), PKDTL WITH(NOLOCK) WHERE PKHDR.PKHDRSEQ = ")
                .append(voTempOrd.getPKID());
        sql.append(" AND PKDTL.ISDEFAULT = 'Y' AND PKHDR.ISKIT = 'Y' AND PKHDR.PKHDRSEQ = PKDTL.PKHDRSEQ");

        RowSet rs = this.dao.Exce_Select(sql);
        if (rs.next()) {
          voTempOrd.setHCPCS(StringUtil.validate(rs.getString("X")));
        }
      }
    }
  }

  public void configurePrice(OrderDataBean orderDataBean, VOTEMPORD voTempOrd, boolean useOriginalTaxable,
      boolean useSKU) throws DAOException, SQLException, MedeqException {
    // Case: 28260
    if (voTempOrd.getRXNUMBER().length() > 0 && this.dao.getCompanyNo().startsWith("DCA")) {
      return;
    }

    PriceUtil price = null;

    if (useSKU) {
      price = new PriceUtil(this.dao, orderDataBean.getOfficeNo(), orderDataBean.getBillID(),
          voTempOrd.getITEMTYPE().equals("SALE") ? "SINV" : "RINV", voTempOrd.getMAKE(),
          voTempOrd.getPARTNO(), voTempOrd.getITEMTYPE().equals("SALE") ? "SINV" : "RINV",
          voTempOrd.getACTUALUOM(), this.today, orderDataBean.getBillPeriod(), orderDataBean.getShipToState(),
          orderDataBean.getShipToZip(),
          StringUtils.isNotBlank(voTempOrd.getINSURANCE()) ? voTempOrd.getINSURANCE()
              : orderDataBean.getInsurance(),
          (orderDataBean.getFileClaim().equals("Y") ? true : false)
              && (voTempOrd.getCLAIM().equals("Y") ? true : false),
          voTempOrd.getMODIFIER1(), voTempOrd.getMODIFIER2(), orderDataBean.getOrderDate(),
          voTempOrd.getSKU(), voTempOrd.getLOTNO());
    } else {
      price = new PriceUtil(this.dao, orderDataBean.getOfficeNo(), orderDataBean.getBillID(),
          voTempOrd.getITEMTYPE().equals("SALE") ? "SINV" : "RINV", voTempOrd.getMAKE(),
          voTempOrd.getPARTNO(), voTempOrd.getITEMTYPE().equals("SALE") ? "SINV" : "RINV",
          voTempOrd.getACTUALUOM(), this.today, orderDataBean.getBillPeriod(), orderDataBean.getShipToState(),
          orderDataBean.getShipToZip(),
          StringUtils.isNotBlank(voTempOrd.getINSURANCE()) ? voTempOrd.getINSURANCE()
              : orderDataBean.getInsurance(),
          (orderDataBean.getFileClaim().equals("Y") ? true : false)
              && (voTempOrd.getCLAIM().equals("Y") ? true : false),
          voTempOrd.getMODIFIER1(), voTempOrd.getMODIFIER2(), orderDataBean.getOrderDate(),
          voTempOrd.getSKU(), voTempOrd.getLOTNO());
    }
    this.setPriceToTempOrd(orderDataBean, voTempOrd, price, useOriginalTaxable);
  }

  public void redirectToItemPage(WebBean sd) throws IOException {
    StringBuilder sMsg = new StringBuilder("win=window.dialogArguments;");
    sMsg.append("win.location.href='/meds/order/show_detail.jsp?reloadtop=Y';");
    sMsg.append("window.close();");
    sd.printJavaScript(sMsg);
  }

  public void sale2RentalUpdate(int sid, String newSKU) throws Exception {
    OrderDataBean orderDataBean = OrderDataBean.getOrderDataBeanFromSession(this.hs);
    VOTEMPORD voTempOrd = TblTEMPORD.getVOByIdentity(this.dao, sid);
    voTempOrd.setSKU(newSKU);
    voTempOrd.setITEMTYPE("REN1");

    ModifierUtil.replace(voTempOrd, "NU", "RR");
    ModifierUtil.appendModifier(voTempOrd, "RR");
    ModifierUtil.remove(voTempOrd, "BP");

    // CHANGE PRICE
    PriceUtil price = new PriceUtil(this.dao, orderDataBean.getOfficeNo(), orderDataBean.getBillID(),
        voTempOrd.getITEMTYPE().equals("SALE") ? "SINV" : "RINV", voTempOrd.getMAKE(), voTempOrd.getPARTNO(),
        voTempOrd.getITEMTYPE().equals("SALE") ? "SINV" : "RINV", voTempOrd.getACTUALUOM(), this.today,
        orderDataBean.getBillPeriod(), orderDataBean.getShipToState(), orderDataBean.getShipToZip(),
        StringUtils.isNotBlank(voTempOrd.getINSURANCE()) ? voTempOrd.getINSURANCE()
            : orderDataBean.getInsurance(),
        (orderDataBean.getFileClaim().equals("Y") ? true : false)
            && (voTempOrd.getCLAIM().equals("Y") ? true : false),
        voTempOrd.getMODIFIER1(), voTempOrd.getMODIFIER2(), orderDataBean.getOrderDate(), voTempOrd.getSKU(),
        voTempOrd.getLOTNO());

    this.setPriceToTempOrd(orderDataBean, voTempOrd, price, true);

    this.setAllowable(orderDataBean, voTempOrd);
    this.configurePrice(orderDataBean, voTempOrd, true, false);
    this.updatePatientResponsibility(orderDataBean, voTempOrd);
    TblTEMPORD.update(this.dao, voTempOrd, this.today, this.sEmployeeNo);

    orderDataBean.getSaleToRentalTempOrdIDs().add(String.valueOf(sid));
  }

  public void sale2RentalUpdate(OrderDataBean orderDataBean)
      throws NumberFormatException, DAOException, SQLException {
    for (String tempOrdID : orderDataBean.getSaleToRentalTempOrdIDs()) {
      VOTEMPORD voTempOrd = TblTEMPORD.getVOByIdentity(this.dao, Integer.parseInt(tempOrdID));
      VOORDDTL voOrdDtl = TblORDDTL.getVOByIdentity(this.dao, voTempOrd.getORDDTLID());

      if (voOrdDtl.getSHIPQTY() - voOrdDtl.getRTNQTY() > 0) {
        // Update INVTORY
        List<String> vSet = new ArrayList<String>();
        List<String> vCriteria = new ArrayList<String>();

        if (!InventoryUtil.isCTitle(this.dao, voOrdDtl.getMAKE(), voOrdDtl.getPARTNO())) {
          vSet.clear();
          vSet.add("INVSTATUS = 'REN1'");
          vSet.add("ONHANDQTY = 0");

          vCriteria.add("SKU = '" + voTempOrd.getSKU() + "'");
          vCriteria.add("OFFICE = '" + voTempOrd.getOFFICE() + "'");
          vCriteria.add("LOTNO = '" + voTempOrd.getLOTNO() + "'");

          TblINVTORY.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);
        }

        // Update TRACKDTL
        vCriteria.clear();
        vCriteria.add("ORDDTLID = " + voTempOrd.getORDDTLID());

        vSet.clear();
        vSet.add("ITEMTYPE = 'REN1'");
        vSet.add("SKU = '" + voTempOrd.getSKU() + "'");

        TblTRACKDTL.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);

        // Update TH
        vCriteria.clear();
        vCriteria.add("SKU = '" + voOrdDtl.getSKU() + "'");
        vCriteria.add("OFFICE = '" + voOrdDtl.getOFFICE() + "'");
        vCriteria.add("LOTNO = '" + voOrdDtl.getLOTNO() + "'");
        vCriteria.add("ORDERNO = '" + voOrdDtl.getORDERNO() + "'");
        vCriteria.add("TRANTYPE = 'DELIVERY'");

        vSet.clear();
        vSet.add("SKU = '" + voTempOrd.getSKU() + "'");
        vSet.add("OFFICE = '" + voTempOrd.getOFFICE() + "'");
        vSet.add("LOTNO = '" + voTempOrd.getLOTNO() + "'");
        vSet.add("THSTATUS = 'REN1'");

        TblTH.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);

        // Update TRACKDTLBREAKOUT
        vCriteria.clear();
        vCriteria.add("SKU = '" + voOrdDtl.getSKU() + "'");
        vCriteria.add("LOTNO = '" + voOrdDtl.getLOTNO() + "'");
        vCriteria.add("ORDDTLID = '" + voOrdDtl.getSYSTEMGENERATEID() + "'");

        vSet.clear();
        vSet.add("SKU = '" + voTempOrd.getSKU() + "'");
        vSet.add("LOTNO = '" + voTempOrd.getLOTNO() + "'");

        TblTRACKDTLBREAKOUT.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);
      }
    }
  }

  public void rentalToSaleRoutine(VOORDDTL voOrdDtlOriginal, boolean fromBilling)
      throws DAOException, SQLException, JustAWarning {
    this.rentalToSaleRoutine(voOrdDtlOriginal, fromBilling, false);
  }

  public void rentalToSaleRoutine(VOORDDTL voOrdDtlOriginal, boolean fromBilling, boolean updateTrackDtl)
      throws DAOException, SQLException, JustAWarning {
    // Update TRACKDTL
    List<String> vCriteria = new ArrayList<String>();
    vCriteria.clear();
    vCriteria.add("ORDDTLID = " + voOrdDtlOriginal.getSYSTEMGENERATEID());

    List<String> vOrder = new ArrayList<String>();
    vOrder.clear();
    vOrder.add("CAST(TRACKNO AS INT) DESC");

    List<VOTRACKDTL> vTrackDtl = TblTRACKDTL.getVOVector(this.dao, vCriteria, vOrder, 1, 1);

    if (vTrackDtl.size() > 0) {
      VOTRACKDTL voTrackDtl = vTrackDtl.get(0);
      voTrackDtl.setITEMTYPE("SALE");

      if (TblTENMONTHANSWER.getVO(this.dao, voOrdDtlOriginal.getSYSTEMGENERATEID()).getANSWER().equals("P")
          && voOrdDtlOriginal.getBILLCTR() >= 14) {
        // Do nothing
      } else {
        if (StringUtils.isNotBlank(voTrackDtl.getLASTBILLDATE()) && (updateTrackDtl || !fromBilling)) {
          voTrackDtl.setSTARTBILLDATE(DateUtil.dateIncreaseByDay(voTrackDtl.getLASTBILLDATE(), 1));
        }

        if (updateTrackDtl || !fromBilling) {
          voTrackDtl.setLASTBILLDATE("");
        }
      }

      TblTRACKDTL.update(this.dao, voTrackDtl, this.today, this.sEmployeeNo);
      // OrderUtil.updateTHCurrentVal(dao, voTrackDtl.getORDDTLID());

      // Case: 32899
      String sku = voTrackDtl.getSKU();
      String office = voTrackDtl.getOFFICE();
      String lotNo = voTrackDtl.getLOTNO();
      String make = voOrdDtlOriginal.getMAKE();
      String partNo = voOrdDtlOriginal.getPARTNO();

      StringBuilder sql = new StringBuilder("SELECT SKU, LOTNO, MAKE, PARTNO");
      sql.append(" FROM TRACKDTLBREAKOUT WITH(NOLOCK)");
      sql.append(" WHERE TRACKNO = ").append(voTrackDtl.getTRACKNO());
      sql.append(" AND ORDDTLID = ").append(voTrackDtl.getORDDTLID());

      RowSet rs = dao.Exce_Select(sql);
      // Assume record count <= 1
      if (rs.next()) {
        sku = StringUtil.validate(rs.getString("SKU"));
        lotNo = StringUtil.validate(rs.getString("LOTNO"));
        make = StringUtil.validate(rs.getString("MAKE"));
        partNo = StringUtil.validate(rs.getString("PARTNO"));
      }

      // Update INVTORY ; Changed by Eddy on 11/30/2005
      vCriteria.clear();
      vCriteria.add("SKU = '" + sku + "'");
      vCriteria.add("OFFICE = '" + office + "'");
      vCriteria.add("LOTNO = '" + lotNo + "'");

      if (!TblINVTORY.isExist(this.dao, vCriteria)) {
        throw new JustAWarning("NO SUCH ITEM: " + sku + " / " + office + " / " + lotNo);
      }

      // If not C-TITLE
      if (voOrdDtlOriginal.getRTNQTY() == 0 && !InventoryUtil.isCTitle(this.dao, make, partNo)) {
        List<String> vSet = new ArrayList<String>();
        vSet.clear();
        vSet.add("INVSTATUS = 'SOLD'");
        TblINVTORY.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);

        /*
         * Account posting. Sells a rental item.
         */
        String customerId = "";
        RowSet rsX = this.dao.Exce_Select(
            "SELECT CUSTOMERID FROM ORDHDR WITH(NOLOCK) WHERE ORDERNO = " + voOrdDtlOriginal.getORDERNO());
        if (rsX.next()) {
          customerId = StringUtils.defaultString(rsX.getString("CUSTOMERID"));
        }

        InvBean invBean = new InvBean();
        invBean.setQty(1);
        invBean.setSku(sku);
        invBean.setLotNo(lotNo);
        invBean.setOffice(office);

        //MED-1686
        new OrderDao(this.dao).insertRentalToSaleAudit(invBean, voTrackDtl, this.today, this.sEmployeeNo);
        int rentalToSaleSysId = dao.getIdentity();

        new AcctUtil(this.hs, this.dao).CoGSPosting(voOrdDtlOriginal.getORDERNO(), this.today.getDate(),
            customerId, Arrays.asList(new InvBean[] { invBean }), rentalToSaleSysId);

              new OrderDao(this.dao).updateRentalToSaleAudit(invBean, rentalToSaleSysId);
      }
    }
  }

  public void rentalToSaleRoutine_reverse(VOORDDTL voOrdDtlOriginal) throws DAOException, SQLException, JustAWarning {
    // Update TRACKDTL
    List<String> vCriteria = new ArrayList<String>();
    vCriteria.clear();
    vCriteria.add("ORDDTLID = " + voOrdDtlOriginal.getSYSTEMGENERATEID());

    List<String> vOrder = new ArrayList<String>();
    vOrder.clear();
    vOrder.add("CAST(TRACKNO AS INT) DESC");

    List<VOTRACKDTL> vTrackDtl = TblTRACKDTL.getVOVector(this.dao, vCriteria, vOrder, 1, 1);

    if (vTrackDtl.size() > 0) {
      VOTRACKDTL voTrackDtl = vTrackDtl.get(0);
      voTrackDtl.setITEMTYPE("REN1");
      TblTRACKDTL.update(this.dao, voTrackDtl, this.today, this.sEmployeeNo);

      String sku = voTrackDtl.getSKU();
      String office = voTrackDtl.getOFFICE();
      String lotNo = voTrackDtl.getLOTNO();
      String make = voOrdDtlOriginal.getMAKE();
      String partNo = voOrdDtlOriginal.getPARTNO();

      StringBuilder sql = new StringBuilder("SELECT SKU, LOTNO, MAKE, PARTNO");
      sql.append(" FROM TRACKDTLBREAKOUT WITH(NOLOCK)");
      sql.append(" WHERE TRACKNO = ").append(voTrackDtl.getTRACKNO());
      sql.append(" AND ORDDTLID = ").append(voTrackDtl.getORDDTLID());

      RowSet rs = dao.Exce_Select(sql);
      // Assume record count <= 1
      if (rs.next()) {
        sku = StringUtil.validate(rs.getString("SKU"));
        lotNo = StringUtil.validate(rs.getString("LOTNO"));
        make = StringUtil.validate(rs.getString("MAKE"));
        partNo = StringUtil.validate(rs.getString("PARTNO"));
      }

      vCriteria.clear();
      vCriteria.add("SKU = '" + sku + "'");
      vCriteria.add("OFFICE = '" + office + "'");
      vCriteria.add("LOTNO = '" + lotNo + "'");

      if (!TblINVTORY.isExist(this.dao, vCriteria)) {
        throw new JustAWarning("NO SUCH ITEM: " + sku + " / " + office + " / " + lotNo);
      }

      // If not C-TITLE
      if (!InventoryUtil.isCTitle(this.dao, make, partNo)) {
        List<String> vSet = new ArrayList<String>();
        vSet.clear();
        vSet.add("INVSTATUS = 'REN1'");
        TblINVTORY.update(this.dao, vSet, vCriteria, this.today, this.sEmployeeNo);

        /*
         * Account posting. Reverses selling a rental item.
         */
        String customerId = "";
        RowSet rsX = this.dao
            .Exce_Select("SELECT CUSTOMERID FROM ORDHDR WHERE ORDERNO = " + voOrdDtlOriginal.getORDERNO());
        if (rsX.next()) {
          customerId = StringUtils.defaultString(rsX.getString("CUSTOMERID"));
        }
        InvBean invBean = new InvBean();
        invBean.setQty(-1);
        invBean.setSku(sku);
        invBean.setLotNo(lotNo);
        invBean.setOffice(office);

        new AcctUtil(this.hs, this.dao).CoGSPosting(voOrdDtlOriginal.getORDERNO(), this.today.getDate(),
            customerId, Arrays.asList(new InvBean[] { invBean }), 0);
      }
    }
  }

  private void addToRequestFile(OrderDataBean orderDataBean, VOORDDTLCommon voOrdDtl)
      throws DAOException, SQLException {
		if (!orderDataBean.getPreAuth().equals("Y") && orderDataBean.isRequestFile() && !voOrdDtl.getSPECIALORDER()
			.equals("Y") && voOrdDtl.getBACKORDER() > 0 && voOrdDtl.getACTUALQTY() > 0) {
      int reqFileQty = 0;

      StringBuilder sql = new StringBuilder("SELECT QTY FROM PODTL WITH(NOLOCK), POHDR WITH(NOLOCK)");
      sql.append(" WHERE POHDR.PONO = PODTL.PONO");
      sql.append(" AND POHDR.STATUS <> 7");
      sql.append(" AND REQID IN (SELECT SYSTEMGENERATEID FROM REQFILE WHERE ORDDTLID = ");
      sql.append(voOrdDtl.getSYSTEMGENERATEID()).append(")");

      RowSet rs = dao.Exce_Select(sql);
      if (rs.next()) {
        reqFileQty = rs.getInt("QTY");
      }

      if (reqFileQty == 0) {
        List<String> vCriteria = new ArrayList<String>();
        vCriteria.add("ORDDTLID = " + voOrdDtl.getSYSTEMGENERATEID());
        TblREQFILE.delete(this.dao, vCriteria);
      }

      int unit = voOrdDtl.getORDQTY() / voOrdDtl.getACTUALQTY();

      VOREQFILE voReqFile = new VOREQFILE();

      if (voOrdDtl.getSPECIALORDER().equals("Y")) {
        voReqFile.setQTY(voOrdDtl.getACTUALQTY() - reqFileQty);
      } else {
        voReqFile.setQTY((int) (Math.ceil((double) voOrdDtl.getBACKORDER() / unit) - reqFileQty));
        if (voReqFile.getQTY() < 1 && voOrdDtl.getBACKORDER() > 0) {
          voReqFile.setQTY(0);
        }
      }

      // Only need to insert when QTY > 0
      if (voReqFile.getQTY() > 0) {
        voReqFile.setPRIORITY(2);
        voReqFile.setFOROFFICE(voOrdDtl.getOFFICE());
        voReqFile.setINVTYPE(voOrdDtl.getITEMTYPE().startsWith("R") ? "RINV" : "SINV");
        voReqFile.setMAKE(voOrdDtl.getMAKE());
        voReqFile.setPARTNO(voOrdDtl.getPARTNO());
        voReqFile.setUOM(voOrdDtl.getACTUALUOM());
        voReqFile.setSOURCE("O");
        voReqFile.setREQDATE(voOrdDtl.getORDERDATE());
        voReqFile.setREMARKS("ORDER/POS BACKORDER");
        voReqFile.setORDDTLID(voOrdDtl.getSYSTEMGENERATEID());
        voReqFile.setFORCUSTOMER(orderDataBean.getCustomerName());

        TblREQFILE.insert(this.dao, voReqFile, this.today, this.sEmployeeNo);
      }
    }
  }

  public void assignDiagnosis(OrderDataBean orderDataBean, VOTEMPORD voTempOrd) {
    StringBuilder sDiag = new StringBuilder();
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis1())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("1");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis2())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("2");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis3())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("3");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis4())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("4");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis5())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("5");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis6())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("6");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis7())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("7");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis8())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("8");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis9())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("9");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis10())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("10");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis11())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("11");
    }
    if (StringUtils.isNotBlank(orderDataBean.getDiagnosis12())) {
      sDiag.append(sDiag.length() > 0 ? "," : "").append("12");
    }

    voTempOrd.setDIAGPOINTER(sDiag.toString());

    // set ICD10 diag pointer
    StringBuilder sDiagIcd10 = new StringBuilder();
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis1())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("1");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis2())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("2");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis3())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("3");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis4())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("4");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis5())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("5");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis6())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("6");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis7())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("7");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis8())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("8");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis9())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("9");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis10())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("10");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis11())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("11");
    }
    if (StringUtils.isNotBlank(orderDataBean.getIcd10Diagnosis12())) {
      sDiagIcd10.append(sDiagIcd10.length() > 0 ? "," : "").append("12");
    }

    voTempOrd.setDIAGPOINTER_ICD10(sDiagIcd10.toString());
  }

  private void savePYMT(OrderDataBean orderDataBean) throws DAOException, SQLException {
    if (!orderDataBean.getPOS().equals("Y") && !orderDataBean.getPreAuth().equals("Y")) {
      List<String> vCriteria = new ArrayList<String>();
      vCriteria.clear();
      vCriteria.add("ORDERNO = '" + orderDataBean.getTempOrderNo() + "'");
      vCriteria.add("CUSTOMERID = '" + orderDataBean.getCustomerID() + "'");
      vCriteria.add("( (MOP<>'CREDIT') OR ( MOP='CREDIT' AND PAYSTATUS = 'APPROVED'))");

      List<VOPOSPAYMENT> vPOSPayment = TblPOSPAYMENT.getVOVector(this.dao, vCriteria);

      String paymentNos = "";

      for (VOPOSPAYMENT voPOSPayment : vPOSPayment) {

        // PYMT table
        VOPYMT voPYMT = new VOPYMT();
        voPYMT.setPAYMENTTYPE("D");
        voPYMT.setOFFICE(this.sOfficeNo);
        voPYMT.setPAYORTYPE("C");

        // Change to BILLID
        voPYMT.setPAYORID(orderDataBean.getBillID());

        String sTemp = "";
        if (voPOSPayment.getMOP().equals("CASH")) {
          sTemp = "CS";
        } else if (voPOSPayment.getMOP().equals("CHECK")) {
          sTemp = "CK";
        } else if (voPOSPayment.getMOP().equals("GIFT")) {
          sTemp = "GC";
        } else if (voPOSPayment.getMOP().equals("CREDIT")) {
          sTemp = "CR";
        }

        voPYMT.setPAYMENTMETHOD(sTemp);
        voPYMT.setPAYMENTDATE(this.local.getDate());
        voPYMT.setPAYMENTTIME(this.local.getTime());
        voPYMT.setPAYMENTSTATUS("A");
        voPYMT.setPAYMENTNOTES("");
        voPYMT.setAMOUNT(voPOSPayment.getAMOUNT());
        voPYMT.setBALANCE(voPOSPayment.getAMOUNT());
        voPYMT.setAMOUNTTENDER(voPOSPayment.getAMOUNT());
        voPYMT.setALLOCAMOUNT(0);
        voPYMT.setOVERPAIDAMOUNT(0);
        voPYMT.setACCOUNTNO(voPOSPayment.getACCOUNTNO());
        try {
          voPYMT.setLASTFOUR(StringUtils.right(
              CreditCardUtil.getDeCryptedAccountNo(this.dao.getCompanyNo(), voPOSPayment.getACCOUNTNO()),
              4));
        } catch (Exception e) {
        }
        voPYMT.setEXPIRE(voPOSPayment.getEXPIRE());
        voPYMT.setREFNO(voPOSPayment.getREFNO());
        voPYMT.setAUTHNO(voPOSPayment.getAUTHNO());
        voPYMT.setCHECKDATE(this.today.getDate());
        voPYMT.setWRITEOFFCODE("");
        voPYMT.setNEWORDERNO(0);
        voPYMT.setNEWORDERNO(orderDataBean.getOrderNo());

        if (orderDataBean.getCOD().equals("C")) {
          voPYMT.setSHOWONRPT("Y");
        }

        voPYMT.setINSYSDATE(today.getDate());
        voPYMT.setINSYSTIME(today.getTime());
        voPYMT.setINSYSUSER(sEmployeeNo);
        voPYMT.setCREDIT_CARD_TOKEN(trimToEmpty(voPOSPayment.getCREDIT_CARD_TOKEN()));
        voPYMT.setAXIALOGID(trimToEmpty(voPOSPayment.getRESPONSEXML()));

        RowSet rsx = this.dao.Exce_Select("SELECT P.PAYMENTNO FROM PYMT AS P WITH(NOLOCK) WHERE P.PAYORID = '"
            + voPYMT.getPAYORID() + "' AND P.PAYMENTTYPE = '" + voPYMT.getPAYMENTTYPE()
            + "' AND P.PAYMENTMETHOD = '" + voPYMT.getPAYMENTMETHOD() + "' AND P.AUTHNO = '"
            + voPYMT.getAUTHNO() + "' AND P.REFNO = '" + voPYMT.getREFNO() + "' AND P.LASTFOUR = '"
            + voPYMT.getLASTFOUR() + "' AND P.AMOUNT = " + voPYMT.getAMOUNT());
        if (!rsx.next()) {
          int iPaymentNo = this.medeqTableUtil.getSeqNumber("PAYMENT", this.sEmployeeNo);
          voPYMT.setPAYMENTNO(iPaymentNo);
          TblPYMT.insert(this.dao, voPYMT, this.today, this.sEmployeeNo);
          new AcctUtil(this.hs, this.dao).PaymentPosting(iPaymentNo);
          paymentNos += (StringUtils.isNotBlank(paymentNos) ? "," : "") + iPaymentNo;
        }
      }

      orderDataBean.setPaymentNos(paymentNos);
    }
  }

  private void savePA(OrderDataBean orderDataBean) throws DAOException, SQLException {
    String sql = "DELETE PA WHERE ORDERNO = " + orderDataBean.getOrderNo() * (orderDataBean.isPreAuth() ? -1 : 1);
    this.dao.Exce_Update_Del_ins(sql);

    for (PaXListOfHcpcs paXListOfHcpcs : orderDataBean.getPreAuthorizations()) {
      paXListOfHcpcs.getPa().setORDERNO(orderDataBean.getOrderNo() * (orderDataBean.isPreAuth() ? -1 : 1));
      TblPA.insert(this.dao, paXListOfHcpcs.getPa());
      paXListOfHcpcs.getPa().setSYSID(this.dao.getIdentity());
    }
  }

  private void saveOrdAuth(OrderDataBean orderDataBean) throws Exception {
    VOORDAUTHCommon voOrdAuth = orderDataBean.getOrdAuth(this.dao);

    // Saving to ORDAUTH table
    if (orderDataBean.getPreAuth().equals("Y")) {
      if (TblPREORDAUTH.isExist(this.dao, voOrdAuth.getORDERNO())) {
        TblPREORDAUTH.update(this.dao, (VOPREORDAUTH) voOrdAuth, this.today, this.sEmployeeNo);
      } else {
        TblPREORDAUTH.insert(this.dao, (VOPREORDAUTH) voOrdAuth, this.today, this.sEmployeeNo);
      }
    } else {
      if (TblORDAUTH.isExist(this.dao, voOrdAuth.getORDERNO())) {
        TblORDAUTH.update(this.dao, (VOORDAUTH) voOrdAuth, this.today, this.sEmployeeNo);
      } else {
        TblORDAUTH.insert(this.dao, (VOORDAUTH) voOrdAuth, this.today, this.sEmployeeNo);
        this.updateOrdAuthAudit((VOORDAUTH) voOrdAuth);
      }
    }
  }

  private void updateOrdAuthAudit(VOORDAUTH voOrdAuth) throws DAOException, SQLException {
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO() + ", 'DRRX', '', '"
        + voOrdAuth.getDRRX() + "', " + this.sEmployeeNo + ", '" + this.today.getDate() + "', '"
        + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'CHECK_INSURANCE_LEGIBILITY', '', '" + voOrdAuth.getCHECK_INSURANCE_LEGIBILITY() + "', "
        + this.sEmployeeNo + ", '" + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'SCHEDULE_EVALUATION_DATE', '', '" + voOrdAuth.getSCHEDULE_EVALUATION_DATE() + "', "
        + this.sEmployeeNo + ", '" + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'EVALUATE_ACTUAL_DATE', '', '" + voOrdAuth.getEVALUATE_ACTUAL_DATE() + "', " + this.sEmployeeNo
        + ", '" + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'GET_BACK_JUSTIFICATION', '', '" + voOrdAuth.getGET_BACK_JUSTIFICATION() + "', " + this.sEmployeeNo
        + ", '" + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'APPROVE_AUTHORIZATION', '', '" + voOrdAuth.getAPPROVE_AUTHORIZATION() + "', " + this.sEmployeeNo
        + ", '" + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'GET_BACK_PA', '', '" + voOrdAuth.getGET_BACK_PA() + "', " + this.sEmployeeNo + ", '"
        + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'VERIFY_COVERAGE_REIMBURSEMENT', '', '" + voOrdAuth.getVERIFY_COVERAGE_REIMBURSEMENT() + "', "
        + this.sEmployeeNo + ", '" + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'MANAGEMENT_REVIEWED', '', '" + voOrdAuth.getMANAGEMENT_REVIEWED() + "', " + this.sEmployeeNo
        + ", '" + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'ASSEMBLY', '', '" + voOrdAuth.getASSEMBLY() + "', " + this.sEmployeeNo + ", '"
        + this.today.getDate() + "', '" + this.today.getTime() + "')");
    this.dao.Exce_Update_Del_ins("INSERT INTO ORDAUTH_AUDIT VALUES (" + voOrdAuth.getORDERNO()
        + ", 'VERIFY_COVERAGE', '', '" + voOrdAuth.getVERIFY_COVERAGE() + "', " + this.sEmployeeNo + ", '"
        + this.today.getDate() + "', '" + this.today.getTime() + "')");
  }

  private VOORDHDRCommon saveOrdHdr(OrderDataBean orderDataBean, List<? extends VOORDDTLCommon> vTempOrd, boolean isZeroBalance)
      throws Exception {
    VOORDHDRCommon voOrdHdr = orderDataBean.getOrdHdr(this.dao, this.local, this.sOfficeNo, this.sEmployeeNo,
        vTempOrd.size(), isZeroBalance);

    if ((CompanyUtil.isMedSouth(dao.getCompanyNo()) || CompanyUtil.isMedeq(dao.getCompanyNo()))
        && vTempOrd.size() > 0) {
      // If all orddtls are marked as hold, update ordhdr.hold to Y
      boolean allHold = true;
      for (VOORDDTLCommon dtl : vTempOrd) {
        if (!dtl.isHOLDDELIVERY()) {
          allHold = false;
          break;
        }
      }
      if (allHold) {
        voOrdHdr.setHOLD("Y");
      }
    }
    if (StringUtils.isNotBlank(voOrdHdr.getMESSAGE()) && StringUtils
        .defaultString(new DefaultUtil(this.hs, this.dao).getParameter("OMC")).equalsIgnoreCase("Y")) {
      RowSet rs = this.dao
          .Exce_Select("SELECT NOTESID FROM NOTESDTL WITH(NOLOCK) WHERE NOTESTYPE = 'C' AND NOTESKEY1 = '"
              + voOrdHdr.getCUSTOMERID() + "' AND NOTESDETAIL = '" + voOrdHdr.getMESSAGE() + "'");
      if (!rs.next()) {
        VONOTESDTL vo = new VONOTESDTL();
        vo.setNOTESID(new MedeqTableUtil(this.dao).getSeqNumber("NOTESID", this.sEmployeeNo));
        vo.setCATEID(1);
        vo.setIMPID(5);
        vo.setNOTESTYPE("C");
        vo.setNOTESKEY1(voOrdHdr.getCUSTOMERID());
        vo.setNOTESKEY2(-1);
        vo.setNOTESSUBJECT("ORDER (#" + voOrdHdr.getORDERNO() + ") MEMO");
        vo.setNOTESDETAIL(voOrdHdr.getMESSAGE());
        vo.setNOTESSTATUS("P");
        vo.setREMIND("0");
        vo.setAUDIT_OFFICE(this.sOfficeNo);
        vo.setAUDIT_USER(this.sEmployeeNo);
        vo.setAUDIT_DATE(this.today.getDate());
        vo.setAUDIT_TIME(this.today.getTime());
        vo.setCREATED_DATE(this.today.getDate());
        vo.setCREATED_TIME(this.today.getTime());
        TblNOTESDTL.insert(this.dao, vo);
      }
    }

    if (voOrdHdr.getCUSTOMERID().length() == 0) {
      throw new JustAWarning("Customer ID Missing, Please Contact Bonafide.");
    }

    // Insert to ORDHDR table
    if (orderDataBean.getPreAuth().equals("Y")) {
      if (orderDataBean.getEdit().equals("Y")) {
        TblPREORDHDR.update(this.dao, (VOPREORDHDR) voOrdHdr, this.today, this.sEmployeeNo);
      } else {
        TblPREORDHDR.insert(this.dao, (VOPREORDHDR) voOrdHdr, this.today, this.sEmployeeNo);
      }
    } else {
      if (orderDataBean.getEdit().equals("Y")) {
        TblORDHDR.update(this.dao, (VOORDHDR) voOrdHdr, this.today, this.sEmployeeNo);
      } else {
        TblORDHDR.insert(this.dao, (VOORDHDR) voOrdHdr, this.today, this.sEmployeeNo);
      }
    }

    return(voOrdHdr);
  }

  public static void updateVendor(DaoUtil dao, String make, String partNo, String officeNo, VOTEMPORD voTempOrd)
      throws DAOException, SQLException {
    if ("Y".equals(voTempOrd.getDROPSHIP())) {
      Formatter f = new Formatter();
      f.format("SELECT TOP 1 A.VENDOR AS A_VENDOR");
      f.format(" FROM INVTORY A WITH(NOLOCK), VENDOR B WITH(NOLOCK)");
      f.format(" WHERE A.MAKE = '%s'", make);
      f.format(" AND A.PARTNO = '%s'", partNo);
      f.format(" AND A.OFFICE = '%s'", officeNo);
      f.format(" AND B.VENDOR = A.VENDOR AND B.DROPSHIP = 'Y'");
      f.format(" ORDER BY A.PURCHASED");

      RowSet rs = dao.Exce_Select(f.toString());
      if (rs.next()) {
        f = new Formatter();
        f.format("UPDATE TEMPORD SET VENDOR = '%s'", rs.getString("A_VENDOR"));
        f.format(" WHERE ORDERNO = %s AND SEQNO = %s", voTempOrd.getORDERNO(), voTempOrd.getSEQNO());
        dao.Exce_Update_Del_ins(f.toString());
      }
    }
  }

  public static List<PaXListOfHcpcs> getAllPreauthorizations(DaoUtil dao, int orderNo, boolean preauth)
      throws DAOException, SQLException {
    List<PaXListOfHcpcs> listOfPaXListOfHcpcs = new ArrayList<>();

    List<String> vCriteria = new ArrayList<>();
    vCriteria.add("ORDERNO = " + orderNo * (preauth ? -1 : 1));

    List<String> vOrder = new ArrayList<>();
    vOrder.add("FROMDATE ASC");
    vOrder.add("TODATE ASC");

    List<VOPA> vPA = TblPA.getVOVector(dao, vCriteria, vOrder);
    for (VOPA pa : vPA) {
      PaXListOfHcpcs paXListOfHcpcs = new PaXListOfHcpcs();
      paXListOfHcpcs.setPa(pa);

      RowSet rsHcpcsList = dao
          .Exce_Select("SELECT DISTINCT HCPCS FROM PA_X_HCPCS WITH(NOLOCK) WHERE DELETED <> '1' AND PASYSID="
              + pa.getSYSID());
      while (rsHcpcsList.next()) {
        String tmp = rsHcpcsList.getString("HCPCS");
        if (isNotBlank(tmp)) {
          paXListOfHcpcs.getListOfHcpcs().add(tmp);
        }
      }

      listOfPaXListOfHcpcs.add(paXListOfHcpcs);
    }

    return listOfPaXListOfHcpcs;
  }
}

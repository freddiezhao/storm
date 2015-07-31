package com.yihaodian.bi.storm.common.model;


import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/** 
 * @desc   JumpMQ推送的订单信息
 * @author weicong
 * @create 2014-1-13 
 * 如果有子单，so_item的信息会在子单上体现
 */

public class JumpMQOrderVo implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -1918686872429040329L;
	
	/**消息发送时间*/
	private Date msgSendTime;
	/**订单ID*/
	private Long id;
	/**用户ID*/
	private Long endUserId;
	/**订单Code*/
	private String orderCode;
	/**订单创建时间*/
	private Date orderCreateTime;
	/**订单类型*/
    private Integer orderType;	
    /**订单状态*/
    private Integer orderStatus;
	/**业务类型*/
	private Integer bizType;
	/**订单来源*/
	private Integer orderSource;
    /**合规  	1：1号店  2：1mall*/	
	private Integer siteType ;
    /**合约机运营商;1(联通) 2(移动)3电信*/
    private Integer mobileOperator;
    /**子单ID列表*/
    private List<Long> childOrderIdList;
    /**父单ID*/
    private Long parentSoId;
    /**数据交换标志位*/
    private Integer dataExchangeFlag;
    /**收货人ID*/
	private Long goodReceiverId;
	/**收货人名称*/
	private String goodReceiverName;
	/**收货人省份*/
	private String goodReceiverProvince;
	/**收货人城市*/
	private String goodReceiverCity;
	/**收货人县*/
	private String goodReceiverCounty;
	/**收货人省份ID*/
	private Long goodReceiverProvinceId;
	/**收货人城市ID*/
	private Long goodReceiverCityId;
	/**收货人县ID*/
	private Long goodReceiverCountyId;
	/**收货人国家ID*/
	private Long goodReceiverCountryId;
	/**收货人4级区域ID*/
	private Long goodReceiverAreaId;
	/**收货人4级区域名称*/
	private String goodReceiverArea;
	/**收货人4级区域详细地址*/
	private String goodReceiverAddress;
	/**收货人邮编*/
	private String goodReceiverPostCode;
	/**收货人手机*/
	private String goodReceiverMobile;
	/**收货人电话*/
	private String goodReceiverPhone;
	/**订单金额*/
	private BigDecimal orderAmount;
	/**商品金额*/
	private BigDecimal productAmount;
	/**运费*/
	private BigDecimal orderDeliveryFee;
	/**返利支付金额*/
	private BigDecimal orderPaidByRebate;
	/**账户支付金额*/
	private BigDecimal orderPaidByAccount;
	/**其他支付金额*/
	private BigDecimal orderPaidByOthers;
	/**抵用券支付金额*/
	private BigDecimal orderPaidByCoupon;
	/**运费抵用券支付金额*/
	private BigDecimal deliveryFeePaidByCoupon;
	/**礼品卡支付金额*/
	private BigDecimal orderPaidByCard;
	/**COD为0, 非COD为订单金额*/
	private BigDecimal accountPayable;
	/**货到应收金额*/
	private BigDecimal collectOnDeliveryAmount;	
	/**支付时间*/
	private Date orderPaymentConfirmDate;
	/**支付标识*/
	private Integer orderPaymentSignal;
	/**支付方式ID*/
	private Long orderPaymentMethodId;
	/**支付网关CODE*/
	private String orderPaymentCode;
	/**支付网关ID*/
	private Long paymentGatewayId;
	/**审单标识*/
	private Integer orderNeedCs;
	/**后台操作人ID*/
	private Long backOperatorId;
	/**订单出库时间*/
	private Date orderToLogisticsTime;
	/**订单出库状态*/
	private Integer orderOutOfInentoryStatus;
	/**配送公司供应商*/
	private String merchantDistSupplier;
	/**配送公司URL*/
	private String deliverySheetCompanyUrl;	
	/**配送单号*/
	private String merchantExpressNbr;	
	/**配送备注*/
	private String deliveryRemark;	
	/**配送人手机号*/
	private String orderDeliveryPersonMobile;
	/**配送人*/
	private String orderDeliveryPerson;	
	/**期望发货时间*/
	private Date expectDeliveryDate;
	/**实际发货时间*/
	private Date deliveryDate;
	/**期望收货日期*/
	private Date expectReceiveDate;
	/**期望收货时间段*/
	private Integer expectReceiveTime;
	/**预计送达日期*/
	private Date estimateReceiveDate;	
	/**收货日期*/
	private Date receiveDate;
	/**配送方式*/
	private Long orderDeliveryMethodId;
	/**供应商处理订单状态*/
	private Integer supplierProcessStatus;
	/**PO ID*/
	private Long poId;
	/**订单取消日期*/
	private Date cancelDate;
	/**是否是叶子订单*/
	private Integer isLeaf;
	/**配送公司名称*/
	private String deliverySheetCompanyName;
	/**用户购买次数*/
	private Long boughtTimes;
	/**是否vip*/
	private Integer isVip;
	/**仓库ID*/
	private Long warehouseId;
	/**配送商ID*/
	private Long deliverySupplierid;
	/**订单所需的积分*/
	private Integer orderNeedIntegral;
	/**是否需要等货*/
	private Integer virtualStockStatus;
	/**订单更新时间*/
	private Date updateTime;
	/**配送方式类型*/
	private Integer deliveryMethodType;
	/**用户选择的配送服务类型*/
	private Integer deliveryServiceType;
	/**支付方式*/
	private Integer payServiceType;
	/**订单重量*/
	private BigDecimal orderWeight;
	/**拆单标记ID*/
	private Long deliveryPartitionId;
	/**延时转DO天数*/
	private Integer preSo2doDays;
	/**1:LBY 2:FBY一地备货 3:FBY多地备货 -1:非LBY(普通的)*/
	private Integer sbyType;
	/**商家ID*/
	private Long merchantId;
	/**so2do时间*/
	private Date soToDoTime;
    /** 取消操作人*/
    private Long cancelOperatorId;
    /**取消原因*/
    private Integer reasonFailure;
    /** 客服备注*/
    private String orderCsRemark;
    /**取消后的 isNeedReturnStockFlag*/
    private Integer isNeedReturnStockFlag;
    /**订单审核失败原因*/
    private String orderCsReason;
    /**订单审核处理人*/
    private String treatmentUserId;
    /**货物打印备注字段*/
    private String goodRemark;
    /**订单审核处理时间*/
    private Date treatmentTime;
	/**订单积分*/
	private BigDecimal realPoint;
	/**满立减金额*/
	private BigDecimal orderPromotionDiscount;
	/**核算运费*/
	private BigDecimal orderAccountingDeliveryFee;
	/**父单*/
	private JumpMQOrderVo parentOrder;
    /**子单列表*/
    private List<JumpMQOrderVo> childOrderList = new ArrayList<JumpMQOrderVo>();
    /**soItem列表*/
    private List<JumpMQOrderItemVo> soItemList = new ArrayList<JumpMQOrderItemVo>();
    /**删除商品删除的soItem列表*/
    private List<JumpMQOrderItemVo> delSoItemList = new ArrayList<JumpMQOrderItemVo>();
    // fields NOT from Message.
    /**订单分类（1：网上支付，2：货到付款）*/
    private Integer paymentCategory;
    
	public Date getMsgSendTime() {
		return msgSendTime;
	}
	public void setMsgSendTime(Date msgSendTime) {
		this.msgSendTime = msgSendTime;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getOrderCode() {
		return orderCode;
	}
	public void setOrderCode(String orderCode) {
		this.orderCode = orderCode;
	}
	public Date getOrderCreateTime() {
		return orderCreateTime;
	}
	public void setOrderCreateTime(Date orderCreateTime) {
		this.orderCreateTime = orderCreateTime;
	}
	public Integer getOrderType() {
		return orderType;
	}
	public void setOrderType(Integer orderType) {
		this.orderType = orderType;
	}
	public Integer getOrderStatus() {
		return orderStatus;
	}
	public void setOrderStatus(Integer orderStatus) {
		this.orderStatus = orderStatus;
	}
	public Integer getBizType() {
		return bizType;
	}
	public void setBizType(Integer bizType) {
		this.bizType = bizType;
	}
	public Integer getOrderSource() {
		return orderSource;
	}
	/**
	 * 
	 * @return platform level_1 ID. 100 stands for mobile and 200 for PC.
	 */
	public Integer getPlatformId() {
		if (orderSource == null) return 200;
		return orderSource == 3 ? 100 : 200;
	}
	public void setOrderSource(Integer orderSource) {
		this.orderSource = orderSource;
	}
	public Integer getSiteType() {
		return siteType;
	}
	public void setSiteType(Integer siteType) {
		this.siteType = siteType;
	}
	public Integer getMobileOperator() {
		return mobileOperator;
	}
	public void setMobileOperator(Integer mobileOperator) {
		this.mobileOperator = mobileOperator;
	}
	public List<Long> getChildOrderIdList() {
		return childOrderIdList;
	}
	public void setChildOrderIdList(List<Long> childOrderIdList) {
		this.childOrderIdList = childOrderIdList;
	}
	public Long getParentSoId() {
		return parentSoId;
	}
	public void setParentSoId(Long parentSoId) {
		this.parentSoId = parentSoId;
	}
	public Integer getDataExchangeFlag() {
		return dataExchangeFlag;
	}
	public void setDataExchangeFlag(Integer dataExchangeFlag) {
		this.dataExchangeFlag = dataExchangeFlag;
	}
	public Long getGoodReceiverId() {
		return goodReceiverId;
	}
	public void setGoodReceiverId(Long goodReceiverId) {
		this.goodReceiverId = goodReceiverId;
	}
	public String getGoodReceiverName() {
		return goodReceiverName;
	}
	public void setGoodReceiverName(String goodReceiverName) {
		this.goodReceiverName = goodReceiverName;
	}
	public String getGoodReceiverProvince() {
		return goodReceiverProvince;
	}
	public void setGoodReceiverProvince(String goodReceiverProvince) {
		this.goodReceiverProvince = goodReceiverProvince;
	}
	public String getGoodReceiverCity() {
		return goodReceiverCity;
	}
	public void setGoodReceiverCity(String goodReceiverCity) {
		this.goodReceiverCity = goodReceiverCity;
	}
	public String getGoodReceiverCounty() {
		return goodReceiverCounty;
	}
	public void setGoodReceiverCounty(String goodReceiverCounty) {
		this.goodReceiverCounty = goodReceiverCounty;
	}
	public Long getGoodReceiverProvinceId() {
		return goodReceiverProvinceId;
	}
	public void setGoodReceiverProvinceId(Long goodReceiverProvinceId) {
		this.goodReceiverProvinceId = goodReceiverProvinceId;
	}
	public Long getGoodReceiverCityId() {
		return goodReceiverCityId;
	}
	public void setGoodReceiverCityId(Long goodReceiverCityId) {
		this.goodReceiverCityId = goodReceiverCityId;
	}
	public Long getGoodReceiverCountyId() {
		return goodReceiverCountyId;
	}
	public void setGoodReceiverCountyId(Long goodReceiverCountyId) {
		this.goodReceiverCountyId = goodReceiverCountyId;
	}
	public Long getGoodReceiverCountryId() {
		return goodReceiverCountryId;
	}
	public void setGoodReceiverCountryId(Long goodReceiverCountryId) {
		this.goodReceiverCountryId = goodReceiverCountryId;
	}
	public Long getGoodReceiverAreaId() {
		return goodReceiverAreaId;
	}
	public void setGoodReceiverAreaId(Long goodReceiverAreaId) {
		this.goodReceiverAreaId = goodReceiverAreaId;
	}
	public String getGoodReceiverArea() {
		return goodReceiverArea;
	}
	public void setGoodReceiverArea(String goodReceiverArea) {
		this.goodReceiverArea = goodReceiverArea;
	}
	public String getGoodReceiverAddress() {
		return goodReceiverAddress;
	}
	public void setGoodReceiverAddress(String goodReceiverAddress) {
		this.goodReceiverAddress = goodReceiverAddress;
	}
	public String getGoodReceiverPostCode() {
		return goodReceiverPostCode;
	}
	public void setGoodReceiverPostCode(String goodReceiverPostCode) {
		this.goodReceiverPostCode = goodReceiverPostCode;
	}
	public String getGoodReceiverMobile() {
		return goodReceiverMobile;
	}
	public void setGoodReceiverMobile(String goodReceiverMobile) {
		this.goodReceiverMobile = goodReceiverMobile;
	}
	public String getGoodReceiverPhone() {
		return goodReceiverPhone;
	}
	public void setGoodReceiverPhone(String goodReceiverPhone) {
		this.goodReceiverPhone = goodReceiverPhone;
	}
	public BigDecimal getOrderAmount() {
		return orderAmount;
	}
	public void setOrderAmount(BigDecimal orderAmount) {
		this.orderAmount = orderAmount;
	}
	public BigDecimal getProductAmount() {
		return productAmount;
	}
	public void setProductAmount(BigDecimal productAmount) {
		this.productAmount = productAmount;
	}
	public BigDecimal getOrderDeliveryFee() {
		return orderDeliveryFee;
	}
	public void setOrderDeliveryFee(BigDecimal orderDeliveryFee) {
		this.orderDeliveryFee = orderDeliveryFee;
	}
	public BigDecimal getOrderPaidByRebate() {
		return orderPaidByRebate;
	}
	public void setOrderPaidByRebate(BigDecimal orderPaidByRebate) {
		this.orderPaidByRebate = orderPaidByRebate;
	}
	public BigDecimal getOrderPaidByAccount() {
		return orderPaidByAccount;
	}
	public void setOrderPaidByAccount(BigDecimal orderPaidByAccount) {
		this.orderPaidByAccount = orderPaidByAccount;
	}
	public BigDecimal getOrderPaidByOthers() {
		return orderPaidByOthers;
	}
	public void setOrderPaidByOthers(BigDecimal orderPaidByOthers) {
		this.orderPaidByOthers = orderPaidByOthers;
	}
	public BigDecimal getOrderPaidByCoupon() {
		return orderPaidByCoupon;
	}
	public void setOrderPaidByCoupon(BigDecimal orderPaidByCoupon) {
		this.orderPaidByCoupon = orderPaidByCoupon;
	}
	public BigDecimal getDeliveryFeePaidByCoupon() {
		return deliveryFeePaidByCoupon;
	}
	public void setDeliveryFeePaidByCoupon(BigDecimal deliveryFeePaidByCoupon) {
		this.deliveryFeePaidByCoupon = deliveryFeePaidByCoupon;
	}
	public BigDecimal getOrderPaidByCard() {
		return orderPaidByCard;
	}
	public void setOrderPaidByCard(BigDecimal orderPaidByCard) {
		this.orderPaidByCard = orderPaidByCard;
	}
	public BigDecimal getAccountPayable() {
		return accountPayable;
	}
	public void setAccountPayable(BigDecimal accountPayable) {
		this.accountPayable = accountPayable;
	}
	public BigDecimal getCollectOnDeliveryAmount() {
		return collectOnDeliveryAmount;
	}
	public void setCollectOnDeliveryAmount(BigDecimal collectOnDeliveryAmount) {
		this.collectOnDeliveryAmount = collectOnDeliveryAmount;
	}
	public Date getOrderPaymentConfirmDate() {
		return orderPaymentConfirmDate;
	}
	public void setOrderPaymentConfirmDate(Date orderPaymentConfirmDate) {
		this.orderPaymentConfirmDate = orderPaymentConfirmDate;
	}
	public Integer getOrderPaymentSignal() {
		return orderPaymentSignal;
	}
	public void setOrderPaymentSignal(Integer orderPaymentSignal) {
		this.orderPaymentSignal = orderPaymentSignal;
	}
	public Long getOrderPaymentMethodId() {
		return orderPaymentMethodId;
	}
	public void setOrderPaymentMethodId(Long orderPaymentMethodId) {
		this.orderPaymentMethodId = orderPaymentMethodId;
	}
	public String getOrderPaymentCode() {
		return orderPaymentCode;
	}
	public void setOrderPaymentCode(String orderPaymentCode) {
		this.orderPaymentCode = orderPaymentCode;
	}
	public Long getPaymentGatewayId() {
		return paymentGatewayId;
	}
	public void setPaymentGatewayId(Long paymentGatewayId) {
		this.paymentGatewayId = paymentGatewayId;
	}
	public Integer getOrderNeedCs() {
		return orderNeedCs;
	}
	public void setOrderNeedCs(Integer orderNeedCs) {
		this.orderNeedCs = orderNeedCs;
	}
	public Long getBackOperatorId() {
		return backOperatorId;
	}
	public void setBackOperatorId(Long backOperatorId) {
		this.backOperatorId = backOperatorId;
	}
	public Date getOrderToLogisticsTime() {
		return orderToLogisticsTime;
	}
	public void setOrderToLogisticsTime(Date orderToLogisticsTime) {
		this.orderToLogisticsTime = orderToLogisticsTime;
	}
	public Integer getOrderOutOfInentoryStatus() {
		return orderOutOfInentoryStatus;
	}
	public void setOrderOutOfInentoryStatus(Integer orderOutOfInentoryStatus) {
		this.orderOutOfInentoryStatus = orderOutOfInentoryStatus;
	}
	public Date getExpectDeliveryDate() {
		return expectDeliveryDate;
	}
	public void setExpectDeliveryDate(Date expectDeliveryDate) {
		this.expectDeliveryDate = expectDeliveryDate;
	}
	public Date getDeliveryDate() {
		return deliveryDate;
	}
	public void setDeliveryDate(Date deliveryDate) {
		this.deliveryDate = deliveryDate;
	}
	public Date getExpectReceiveDate() {
		return expectReceiveDate;
	}
	public void setExpectReceiveDate(Date expectReceiveDate) {
		this.expectReceiveDate = expectReceiveDate;
	}
	public Integer getExpectReceiveTime() {
		return expectReceiveTime;
	}
	public void setExpectReceiveTime(Integer expectReceiveTime) {
		this.expectReceiveTime = expectReceiveTime;
	}
	public Date getReceiveDate() {
		return receiveDate;
	}
	public void setReceiveDate(Date receiveDate) {
		this.receiveDate = receiveDate;
	}
	public Long getOrderDeliveryMethodId() {
		return orderDeliveryMethodId;
	}
	public void setOrderDeliveryMethodId(Long orderDeliveryMethodId) {
		this.orderDeliveryMethodId = orderDeliveryMethodId;
	}
	public Integer getSupplierProcessStatus() {
		return supplierProcessStatus;
	}
	public void setSupplierProcessStatus(Integer supplierProcessStatus) {
		this.supplierProcessStatus = supplierProcessStatus;
	}
	public Long getPoId() {
		return poId;
	}
	public void setPoId(Long poId) {
		this.poId = poId;
	}
	public Date getCancelDate() {
		return cancelDate;
	}
	public void setCancelDate(Date cancelDate) {
		this.cancelDate = cancelDate;
	}
	public Integer getIsLeaf() {
		return isLeaf;
	}
	public void setIsLeaf(Integer isLeaf) {
		this.isLeaf = isLeaf;
	}
	public String getDeliverySheetCompanyName() {
		return deliverySheetCompanyName;
	}
	public void setDeliverySheetCompanyName(String deliverySheetCompanyName) {
		this.deliverySheetCompanyName = deliverySheetCompanyName;
	}
	public Long getBoughtTimes() {
		return boughtTimes;
	}
	public void setBoughtTimes(Long boughtTimes) {
		this.boughtTimes = boughtTimes;
	}
	public Integer getIsVip() {
		return isVip;
	}
	public void setIsVip(Integer isVip) {
		this.isVip = isVip;
	}
	public Long getWarehouseId() {
		return warehouseId;
	}
	public void setWarehouseId(Long warehouseId) {
		this.warehouseId = warehouseId;
	}
	public Long getDeliverySupplierid() {
		return deliverySupplierid;
	}
	public void setDeliverySupplierid(Long deliverySupplierid) {
		this.deliverySupplierid = deliverySupplierid;
	}
	public Integer getOrderNeedIntegral() {
		return orderNeedIntegral;
	}
	public void setOrderNeedIntegral(Integer orderNeedIntegral) {
		this.orderNeedIntegral = orderNeedIntegral;
	}
	public Integer getVirtualStockStatus() {
		return virtualStockStatus;
	}
	public void setVirtualStockStatus(Integer virtualStockStatus) {
		this.virtualStockStatus = virtualStockStatus;
	}
	public Date getUpdateTime() {
		return updateTime;
	}
	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	public Integer getDeliveryMethodType() {
		return deliveryMethodType;
	}
	public void setDeliveryMethodType(Integer deliveryMethodType) {
		this.deliveryMethodType = deliveryMethodType;
	}
	public Integer getDeliveryServiceType() {
		return deliveryServiceType;
	}
	public void setDeliveryServiceType(Integer deliveryServiceType) {
		this.deliveryServiceType = deliveryServiceType;
	}
	public Integer getPayServiceType() {
		return payServiceType;
	}
	public void setPayServiceType(Integer payServiceType) {
		this.payServiceType = payServiceType;
	}
	public BigDecimal getOrderWeight() {
		return orderWeight;
	}
	public void setOrderWeight(BigDecimal orderWeight) {
		this.orderWeight = orderWeight;
	}
	public Long getDeliveryPartitionId() {
		return deliveryPartitionId;
	}
	public void setDeliveryPartitionId(Long deliveryPartitionId) {
		this.deliveryPartitionId = deliveryPartitionId;
	}
	public Integer getPreSo2doDays() {
		return preSo2doDays;
	}
	public void setPreSo2doDays(Integer preSo2doDays) {
		this.preSo2doDays = preSo2doDays;
	}
	public Integer getSbyType() {
		return sbyType;
	}
	public void setSbyType(Integer sbyType) {
		this.sbyType = sbyType;
	}
	public Long getMerchantId() {
		return merchantId;
	}
	public void setMerchantId(Long merchantId) {
		this.merchantId = merchantId;
	}
	public List<JumpMQOrderVo> getChildOrderList() {
		return childOrderList;
	}
	public void setChildOrderList(List<JumpMQOrderVo> childOrderList) {
		this.childOrderList = childOrderList;
	}
	public Long getEndUserId() {
		return endUserId;
	}
	public void setEndUserId(Long endUserId) {
		this.endUserId = endUserId;
	}
	public String getMerchantDistSupplier() {
		return merchantDistSupplier;
	}
	public void setMerchantDistSupplier(String merchantDistSupplier) {
		this.merchantDistSupplier = merchantDistSupplier;
	}
	public String getMerchantExpressNbr() {
		return merchantExpressNbr;
	}
	public void setMerchantExpressNbr(String merchantExpressNbr) {
		this.merchantExpressNbr = merchantExpressNbr;
	}
	public String getDeliveryRemark() {
		return deliveryRemark;
	}
	public void setDeliveryRemark(String deliveryRemark) {
		this.deliveryRemark = deliveryRemark;
	}
	public String getDeliverySheetCompanyUrl() {
		return deliverySheetCompanyUrl;
	}
	public void setDeliverySheetCompanyUrl(String deliverySheetCompanyUrl) {
		this.deliverySheetCompanyUrl = deliverySheetCompanyUrl;
	}
	public String getOrderDeliveryPersonMobile() {
		return orderDeliveryPersonMobile;
	}
	public void setOrderDeliveryPersonMobile(String orderDeliveryPersonMobile) {
		this.orderDeliveryPersonMobile = orderDeliveryPersonMobile;
	}
	public String getOrderDeliveryPerson() {
		return orderDeliveryPerson;
	}
	public void setOrderDeliveryPerson(String orderDeliveryPerson) {
		this.orderDeliveryPerson = orderDeliveryPerson;
	}
	public Date getEstimateReceiveDate() {
		return estimateReceiveDate;
	}
	public void setEstimateReceiveDate(Date estimateReceiveDate) {
		this.estimateReceiveDate = estimateReceiveDate;
	}
    public Date getSoToDoTime(){
        return soToDoTime;
    }
    public void setSoToDoTime(Date soToDoTime){
        this.soToDoTime = soToDoTime;
    }
    public Long getCancelOperatorId(){
        return cancelOperatorId;
    }
    public void setCancelOperatorId(Long cancelOperatorId){
        this.cancelOperatorId = cancelOperatorId;
    }
    public Integer getReasonFailure(){
        return reasonFailure;
    }
    public void setReasonFailure(Integer reasonFailure){
        this.reasonFailure = reasonFailure;
    }
    public String getOrderCsRemark(){
        return orderCsRemark;
    }
    public void setOrderCsRemark(String orderCsRemark){
        this.orderCsRemark = orderCsRemark;
    }
    public Integer getIsNeedReturnStockFlag(){
        return isNeedReturnStockFlag;
    }
    public void setIsNeedReturnStockFlag(Integer isNeedReturnStockFlag){
        this.isNeedReturnStockFlag = isNeedReturnStockFlag;
    }
	public String getOrderCsReason() {
		return orderCsReason;
	}
	public void setOrderCsReason(String orderCsReason) {
		this.orderCsReason = orderCsReason;
	}
	public String getTreatmentUserId() {
		return treatmentUserId;
	}
	public void setTreatmentUserId(String treatmentUserId) {
		this.treatmentUserId = treatmentUserId;
	}
	public String getGoodRemark() {
		return goodRemark;
	}
	public void setGoodRemark(String goodRemark) {
		this.goodRemark = goodRemark;
	}
	public Date getTreatmentTime() {
		return treatmentTime;
	}
	public void setTreatmentTime(Date treatmentTime) {
		this.treatmentTime = treatmentTime;
	}
	public BigDecimal getRealPoint() {
		return realPoint;
	}
	public void setRealPoint(BigDecimal realPoint) {
		this.realPoint = realPoint;
	}
	public BigDecimal getOrderPromotionDiscount() {
		return orderPromotionDiscount;
	}
	public void setOrderPromotionDiscount(BigDecimal orderPromotionDiscount) {
		this.orderPromotionDiscount = orderPromotionDiscount;
	}
	public BigDecimal getOrderAccountingDeliveryFee() {
		return orderAccountingDeliveryFee;
	}
	public void setOrderAccountingDeliveryFee(BigDecimal orderAccountingDeliveryFee) {
		this.orderAccountingDeliveryFee = orderAccountingDeliveryFee;
	}
	public JumpMQOrderVo getParentOrder() {
		return parentOrder;
	}
	public void setParentOrder(JumpMQOrderVo parentOrder) {
		this.parentOrder = parentOrder;
	}
	public List<JumpMQOrderItemVo> getSoItemList() {
		return soItemList;
	}
	public void setSoItemList(List<JumpMQOrderItemVo> soItemList) {
		this.soItemList = soItemList;
	}
	public List<JumpMQOrderItemVo> getDelSoItemList() {
		return delSoItemList;
	}
	public void setDelSoItemList(List<JumpMQOrderItemVo> delSoItemList) {
		this.delSoItemList = delSoItemList;
	}
    public Integer getPaymentCategory() {
		return paymentCategory;
	}
	public void setPaymentCategory(Integer paymentCategory) {
		this.paymentCategory = paymentCategory;
	}
	@Override
    public String toString() {
        return "JumpMQOrderVo [msgSendTime=" + msgSendTime + ", id=" + id + ", endUserId=" + endUserId + ", orderCode="
               + orderCode + ", orderCreateTime=" + orderCreateTime + ", orderType=" + orderType + ", orderStatus="
               + orderStatus + ", bizType=" + bizType + ", orderSource=" + orderSource + ", siteType=" + siteType
               + ", mobileOperator=" + mobileOperator + ", childOrderIdList=" + childOrderIdList + ", parentSoId="
               + parentSoId + ", dataExchangeFlag=" + dataExchangeFlag + ", goodReceiverId=" + goodReceiverId
               + ", goodReceiverName=" + goodReceiverName + ", goodReceiverProvince=" + goodReceiverProvince
               + ", goodReceiverCity=" + goodReceiverCity + ", goodReceiverCounty=" + goodReceiverCounty
               + ", goodReceiverProvinceId=" + goodReceiverProvinceId + ", goodReceiverCityId=" + goodReceiverCityId
               + ", goodReceiverCountyId=" + goodReceiverCountyId + ", goodReceiverCountryId=" + goodReceiverCountryId
               + ", goodReceiverAreaId=" + goodReceiverAreaId + ", goodReceiverArea=" + goodReceiverArea
               + ", goodReceiverAddress=" + goodReceiverAddress + ", goodReceiverPostCode=" + goodReceiverPostCode
               + ", goodReceiverMobile=" + goodReceiverMobile + ", goodReceiverPhone=" + goodReceiverPhone
               + ", orderAmount=" + orderAmount + ", productAmount=" + productAmount + ", orderDeliveryFee="
               + orderDeliveryFee + ", orderPaidByRebate=" + orderPaidByRebate + ", orderPaidByAccount="
               + orderPaidByAccount + ", orderPaidByOthers=" + orderPaidByOthers + ", orderPaidByCoupon="
               + orderPaidByCoupon + ", deliveryFeePaidByCoupon=" + deliveryFeePaidByCoupon + ", orderPaidByCard="
               + orderPaidByCard + ", accountPayable=" + accountPayable + ", collectOnDeliveryAmount="
               + collectOnDeliveryAmount + ", orderPaymentConfirmDate=" + orderPaymentConfirmDate
               + ", orderPaymentSignal=" + orderPaymentSignal + ", orderPaymentMethodId=" + orderPaymentMethodId
               + ", orderPaymentCode=" + orderPaymentCode + ", paymentGatewayId=" + paymentGatewayId + ", orderNeedCs="
               + orderNeedCs + ", backOperatorId=" + backOperatorId + ", orderToLogisticsTime=" + orderToLogisticsTime
               + ", orderOutOfInentoryStatus=" + orderOutOfInentoryStatus + ", merchantDistSupplier="
               + merchantDistSupplier + ", deliverySheetCompanyUrl=" + deliverySheetCompanyUrl
               + ", merchantExpressNbr=" + merchantExpressNbr + ", deliveryRemark=" + deliveryRemark
               + ", orderDeliveryPersonMobile=" + orderDeliveryPersonMobile + ", orderDeliveryPerson="
               + orderDeliveryPerson + ", expectDeliveryDate=" + expectDeliveryDate + ", deliveryDate=" + deliveryDate
               + ", expectReceiveDate=" + expectReceiveDate + ", expectReceiveTime=" + expectReceiveTime
               + ", estimateReceiveDate=" + estimateReceiveDate + ", receiveDate=" + receiveDate
               + ", orderDeliveryMethodId=" + orderDeliveryMethodId + ", supplierProcessStatus="
               + supplierProcessStatus + ", poId=" + poId + ", cancelDate=" + cancelDate + ", isLeaf=" + isLeaf
               + ", deliverySheetCompanyName=" + deliverySheetCompanyName + ", boughtTimes=" + boughtTimes + ", isVip="
               + isVip + ", warehouseId=" + warehouseId + ", deliverySupplierid=" + deliverySupplierid
               + ", orderNeedIntegral=" + orderNeedIntegral + ", virtualStockStatus=" + virtualStockStatus
               + ", updateTime=" + updateTime + ", deliveryMethodType=" + deliveryMethodType + ", deliveryServiceType="
               + deliveryServiceType + ", payServiceType=" + payServiceType + ", orderWeight=" + orderWeight
               + ", deliveryPartitionId=" + deliveryPartitionId + ", preSo2doDays=" + preSo2doDays + ", sbyType="
               + sbyType + ", merchantId=" + merchantId + ", soToDoTime=" + soToDoTime + ", cancelOperatorId="
               + cancelOperatorId + ", reasonFailure=" + reasonFailure + ", orderCsRemark=" + orderCsRemark
               + ", isNeedReturnStockFlag=" + isNeedReturnStockFlag + ", orderCsReason=" + orderCsReason
               + ", treatmentUserId=" + treatmentUserId + ", goodRemark=" + goodRemark + ", treatmentTime="
               + treatmentTime + ", realPoint=" + realPoint + ", orderPromotionDiscount=" + orderPromotionDiscount
               + ", orderAccountingDeliveryFee=" + orderAccountingDeliveryFee + ", parentOrder=" + parentOrder
               + ", childOrderList=" + childOrderList + ", soItemList=" + soItemList + ", delSoItemList="
               + delSoItemList + "]";
    }
	
	public String baseInfo() {
		return "{msg_send_time:" + this.getMsgSendTime()
				+ ", order_id:" + this.getId() 
				+ ", pay_service_type:"  + this.payServiceType
				+ ", end_user_id:" + this.endUserId
				+ ", pay_catagory:" + this.paymentCategory
				+ ", order_create_time: " + this.orderCreateTime
				+ ", order_pay_time:" + this.orderPaymentConfirmDate
				+ ", cancel_time:" + this.cancelDate
				+ "}";
	}
}

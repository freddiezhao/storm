package com.yihaodian.bi.storm.common.model;


import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JumpMQOrderItemVo implements Serializable {
	
	private static final long serialVersionUID = -8646343097951377903L;
	
	private Long id;
    /** 用户ID*/
	private Long endUserId;
	/** 订单ID*/
    private Long orderId;
    /** 产品ID*/
    private Long productId;
    /** 商家ID*/
    private Long merchantId;
    /** 商品金额*/
    private BigDecimal orderItemAmount;
    /** 商品价格*/
    private BigDecimal orderItemPrice;
    /** 商品数量*/
    private Integer orderItemNum;
    /** 父商品ID*/
    private Long parentSoItemId;
    /** 是否是子品*/
    private Integer isItemLeaf;
    /** 仓库ID*/
    private Long warehouseId;
	/** 分摊运费*/
    private BigDecimal deliveryFeeAmount;
	/** 结算价*/
    private BigDecimal settlementPrice;    
	/** 核算运费*/
    private BigDecimal accountingDeliveryFeeAmount;
    /** 产品类型*/
    private Integer productType;
    /** 产品单位积分*/
    private Integer itemPoint = 0;
    /** 商品需扣减积分*/
    private Integer integral;
    /** 商品需扣减积分*购买数量*/
    private Integer totalIntegral;
    /** 活动促销ID */
    private Long priceRuleId;
	/** 促销分摊金额*/
    private BigDecimal promotionAmount;
    /** 分摊到此soItem的抵用券金额*/
    private BigDecimal couponAmount;
    /** 商家抵用券分摊金额*/
    private BigDecimal couponAmountMerchant	;
    /** 运费抵用卷分摊金额 */
    private BigDecimal deliveryFeeCouponAmount;
    /** 节省的金额*/
    private BigDecimal discountAmount;
    /** 冻结的实际库存数量*/
    private BigDecimal frozenRealStockNum;
	/**商品CODE*/
	private String pmInfoCode;
    /** 创建时间 */
    private Date createTime;
    /** 购买该产品获得的返利金额*/
    private BigDecimal orderItemRebate;
    /** 订单明细类型 1:normal order 2:订购订单 3:只付了订金*/
    private Integer orderItemType;
    /** 商家商品id（pmInfoId）*/
    private Long productMerchantId;
    /** 供应商id*/
    private Long supplierId;
    /** 产品供应商id*/
    private Long productSupplierId;
    /** 促销规则类型0:普通；1:清仓；2:团购*/
    private Integer ruleType;
    /** 子品*/
    private List<JumpMQOrderItemVo> subSoItemList = new ArrayList<JumpMQOrderItemVo>();
    
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public Long getEndUserId() {
		return endUserId;
	}
	public void setEndUserId(Long endUserId) {
		this.endUserId = endUserId;
	}
	public Long getOrderId() {
		return orderId;
	}
	public void setOrderId(Long orderId) {
		this.orderId = orderId;
	}
	public Long getProductId() {
		return productId;
	}
	public void setProductId(Long productId) {
		this.productId = productId;
	}
	public Long getMerchantId() {
		return merchantId;
	}
	public void setMerchantId(Long merchantId) {
		this.merchantId = merchantId;
	}
	public BigDecimal getOrderItemAmount() {
		return orderItemAmount;
	}
	public void setOrderItemAmount(BigDecimal orderItemAmount) {
		this.orderItemAmount = orderItemAmount;
	}
	public BigDecimal getOrderItemPrice() {
		return orderItemPrice;
	}
	public void setOrderItemPrice(BigDecimal orderItemPrice) {
		this.orderItemPrice = orderItemPrice;
	}
	public Integer getOrderItemNum() {
		return orderItemNum;
	}
	public void setOrderItemNum(Integer orderItemNum) {
		this.orderItemNum = orderItemNum;
	}
	public Long getParentSoItemId() {
		return parentSoItemId;
	}
	public void setParentSoItemId(Long parentSoItemId) {
		this.parentSoItemId = parentSoItemId;
	}
	public Integer getIsItemLeaf() {
		return isItemLeaf;
	}
	public void setIsItemLeaf(Integer isItemLeaf) {
		this.isItemLeaf = isItemLeaf;
	}
	public Long getWarehouseId() {
		return warehouseId;
	}
	public void setWarehouseId(Long warehouseId) {
		this.warehouseId = warehouseId;
	}
	public BigDecimal getDeliveryFeeAmount() {
		return deliveryFeeAmount;
	}
	public void setDeliveryFeeAmount(BigDecimal deliveryFeeAmount) {
		this.deliveryFeeAmount = deliveryFeeAmount;
	}
	public BigDecimal getSettlementPrice() {
		return settlementPrice;
	}
	public void setSettlementPrice(BigDecimal settlementPrice) {
		this.settlementPrice = settlementPrice;
	}
	public BigDecimal getAccountingDeliveryFeeAmount() {
		return accountingDeliveryFeeAmount;
	}
	public void setAccountingDeliveryFeeAmount(BigDecimal accountingDeliveryFeeAmount) {
		this.accountingDeliveryFeeAmount = accountingDeliveryFeeAmount;
	}
	public Integer getProductType() {
		return productType;
	}
	public void setProductType(Integer productType) {
		this.productType = productType;
	}
	public Integer getItemPoint() {
		return itemPoint;
	}
	public void setItemPoint(Integer itemPoint) {
		this.itemPoint = itemPoint;
	}
	public Integer getIntegral() {
		return integral;
	}
	public void setIntegral(Integer integral) {
		this.integral = integral;
	}
	public Long getPriceRuleId() {
		return priceRuleId;
	}
	public void setPriceRuleId(Long priceRuleId) {
		this.priceRuleId = priceRuleId;
	}
	public Integer getTotalIntegral() {
		return totalIntegral;
	}
	public void setTotalIntegral(Integer totalIntegral) {
		this.totalIntegral = totalIntegral;
	}
	public BigDecimal getPromotionAmount() {
		return promotionAmount;
	}
	public void setPromotionAmount(BigDecimal promotionAmount) {
		this.promotionAmount = promotionAmount;
	}
	public BigDecimal getCouponAmount() {
		return couponAmount;
	}
	public void setCouponAmount(BigDecimal couponAmount) {
		this.couponAmount = couponAmount;
	}
	public BigDecimal getCouponAmountMerchant() {
		return couponAmountMerchant;
	}
	public void setCouponAmountMerchant(BigDecimal couponAmountMerchant) {
		this.couponAmountMerchant = couponAmountMerchant;
	}
	public BigDecimal getDeliveryFeeCouponAmount() {
		return deliveryFeeCouponAmount;
	}
	public void setDeliveryFeeCouponAmount(BigDecimal deliveryFeeCouponAmount) {
		this.deliveryFeeCouponAmount = deliveryFeeCouponAmount;
	}
	public BigDecimal getDiscountAmount() {
		return discountAmount;
	}
	public void setDiscountAmount(BigDecimal discountAmount) {
		this.discountAmount = discountAmount;
	}
	public BigDecimal getFrozenRealStockNum() {
		return frozenRealStockNum;
	}
	public void setFrozenRealStockNum(BigDecimal frozenRealStockNum) {
		this.frozenRealStockNum = frozenRealStockNum;
	}
	public String getPmInfoCode() {
		return pmInfoCode;
	}
	public void setPmInfoCode(String pmInfoCode) {
		this.pmInfoCode = pmInfoCode;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	public BigDecimal getOrderItemRebate() {
		return orderItemRebate;
	}
	public void setOrderItemRebate(BigDecimal orderItemRebate) {
		this.orderItemRebate = orderItemRebate;
	}
	public Integer getOrderItemType() {
		return orderItemType;
	}
	public void setOrderItemType(Integer orderItemType) {
		this.orderItemType = orderItemType;
	}
	public Long getProductMerchantId() {
		return productMerchantId;
	}
	public void setProductMerchantId(Long productMerchantId) {
		this.productMerchantId = productMerchantId;
	}
	public Long getSupplierId() {
		return supplierId;
	}
	public void setSupplierId(Long supplierId) {
		this.supplierId = supplierId;
	}
	public Long getProductSupplierId() {
		return productSupplierId;
	}
	public void setProductSupplierId(Long productSupplierId) {
		this.productSupplierId = productSupplierId;
	}
	public Integer getRuleType() {
		return ruleType;
	}
	/**
	 * Promotion rule type is used for groupon and mp.
	 * 
	 * @return 2 represents groupon, 7 represents mp and 0 represents else.
	 */
	public Integer getPromotionRuleType() {
		if (ruleType == null)
			return 0;
		else if (ruleType != 2 && ruleType != 7)
			return 0;
		else
			return ruleType;
	}
	public void setRuleType(Integer ruleType) {
		this.ruleType = ruleType;
	}
	public List<JumpMQOrderItemVo> getSubSoItemList() {
		return subSoItemList;
	}
	public void setSubSoItemList(List<JumpMQOrderItemVo> subSoItemList) {
		this.subSoItemList = subSoItemList;
	}
	
	public String baseInfo() {
		return "{so_item_id:" + this.getId()
				+ ", order_id:" + this.getOrderId()
				+ ", is_item_leaf:"  + this.getIsItemLeaf()
				+ ", end_user_id:" + this.getEndUserId()
				+ ", item_amt: " + this.getOrderItemAmount()
				+ ", integral:" + this.getIntegral()
				+ "}";
	}
    
}

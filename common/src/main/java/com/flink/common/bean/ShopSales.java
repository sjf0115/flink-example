package com.flink.common.bean;

import com.google.gson.annotations.SerializedName;

public class ShopSales {
    @SerializedName("product_id")
    private Integer productId;
    private String category;
    private Integer price;
    private Long timestamp;

    public ShopSales() {
    }

    public ShopSales(Integer productId, String category, Integer price, Long timestamp) {
        this.productId = productId;
        this.category = category;
        this.price = price;
        this.timestamp = timestamp;
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ShopSales{" +
                "productId=" + productId +
                ", category='" + category + '\'' +
                ", price=" + price +
                ", timestamp=" + timestamp +
                '}';
    }
}

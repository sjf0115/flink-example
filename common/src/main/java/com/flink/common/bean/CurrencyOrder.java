package com.flink.common.bean;

import java.io.Serializable;

public class CurrencyOrder implements Serializable {
    private String orderTime;
    private double amount;
    private String currency;

    public CurrencyOrder() {
    }

    public CurrencyOrder(String orderTime, double amount, String currency) {
        this.orderTime = orderTime;
        this.amount = amount;
        this.currency = currency;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }
}

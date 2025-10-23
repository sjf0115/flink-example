package com.flink.common.bean;

import java.io.Serializable;

public class CurrencyRates implements Serializable {
    private Long updateTime;
    private String currency;
    private double rate;

    public CurrencyRates() {
    }

    public CurrencyRates(Long updateTime, String currency, double rate) {
        this.updateTime = updateTime;
        this.currency = currency;
        this.rate = rate;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }
}

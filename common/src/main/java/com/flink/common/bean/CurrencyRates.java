package com.flink.common.bean;

import java.io.Serializable;

public class CurrencyRates implements Serializable {
    private String updateTime;
    private String currency;
    private double rate;

    public CurrencyRates() {
    }

    public CurrencyRates(String updateTime, String currency, double rate) {
        this.updateTime = updateTime;
        this.currency = currency;
        this.rate = rate;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
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

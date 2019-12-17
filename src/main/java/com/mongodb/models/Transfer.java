package com.mongodb.models;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class Transfer {

    private Date timestamp;
    private String from;
    private String to;
    private Integer amount;

    public Transfer() {
    }

    public Transfer(Date timestamp, String from, String to, Integer amount) {
        this.timestamp = timestamp;
        this.from = from;
        this.to = to;
        this.amount = amount;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transfer transfer = (Transfer) o;
        return Objects.equals(getTimestamp(), transfer.getTimestamp()) &&
                Objects.equals(getFrom(), transfer.getFrom()) &&
                Objects.equals(getTo(), transfer.getTo()) &&
                Objects.equals(getAmount(), transfer.getAmount());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getTimestamp(), getFrom(), getTo(), getAmount());
    }

    @Override
    public String toString() {
        return "Transfer{" +
                "timestamp=" +
                new SimpleDateFormat("yyyy-MM-dd").format(timestamp) +
                ", from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", amount=" + amount +
                '}';
    }
}

package com.ebigdata.hadoop.mr.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Access implements Writable {

    private String phone;
    private long up;
    private long down;
    private long sum;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phone = in.readUTF();
        up = in.readLong();
        down = in.readLong();
        sum = in.readLong();
    }

    public Access() {}

    public Access(String phone, long up, long down, long sum) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getPhone() {
        return phone;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getUp() {
        return up;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getDown() {
        return down;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public long getSum() {
        return sum;
    }

    @Override
    public String toString() {
        return "Access{" +
                "phone='" + phone + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }
}

package entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataTotalWritable implements Writable {

    // 上行数据包总数
    private long upPackNum ;
    // 下行数据包总数
    private long downPackNum ;
    // 上行总流量
    private long upPayLoad ;
    // 下行总流量
    private long downPayLoad ;

    public DataTotalWritable() {
    }

    public DataTotalWritable(long upPackNum, long downPackNum, long upPayLoad, long downPayLoad) {
        this.set(upPackNum, downPackNum, upPayLoad, downPayLoad);
    }

    public void set (long upPackNum, long downPackNum, long upPayLoad,long downPayLoad) {
        this.upPackNum = upPackNum;
        this.downPackNum = downPackNum;
        this.upPayLoad = upPayLoad;
        this.downPayLoad = downPayLoad;
    }

    public long getUpPackNum() {
        return upPackNum;
    }

    public void setUpPackNum(long upPackNum) {
        this.upPackNum = upPackNum;
    }

    public long getDownPackNum() {
        return downPackNum;
    }

    public void setDownPackNum(long downPackNum) {
        this.downPackNum = downPackNum;
    }

    public long getUpPayLoad() {
        return upPayLoad;
    }

    public void setUpPayLoad(long upPayLoad) {
        this.upPayLoad = upPayLoad;
    }

    public long getDownPayLoad() {
        return downPayLoad;
    }

    public void setDownPayLoad(long downPayLoad) {
        this.downPayLoad = downPayLoad;
    }

    //^为异或运算, << 带符号左移, >>带符号右移, >>> 无符号右移
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (downPackNum ^ (downPackNum >>> 32));
        result = prime * result + (int) (downPayLoad ^ (downPayLoad >>> 32));
        result = prime * result + (int) (upPackNum ^ (upPackNum >>> 32));
        result = prime * result + (int) (upPayLoad ^ (upPayLoad >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DataTotalWritable other = (DataTotalWritable) obj;
        if (downPackNum != other.downPackNum)
            return false;
        if (downPayLoad != other.downPayLoad)
            return false;
        if (upPackNum != other.upPackNum)
            return false;
        if (upPayLoad != other.upPayLoad)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return upPackNum + "\t"    + downPackNum + "\t"
                + upPayLoad + "\t"    + downPayLoad ;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(upPackNum);
        out.writeLong(downPackNum);
        out.writeLong(upPayLoad);
        out.writeLong(downPayLoad);
    }

    public void readFields(DataInput in) throws IOException {
        this.upPackNum = in.readLong() ;
        this.downPackNum = in.readLong() ;
        this.upPayLoad = in.readLong() ;
        this.downPayLoad = in.readLong() ;
    }

}
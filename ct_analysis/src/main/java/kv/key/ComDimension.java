package kv.key;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/20 13:50
 */

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//联系人和时间的维度
public class ComDimension extends BaseDimension {

    //联系人维度
    private ContactDimension contactDimension = new ContactDimension();
    //时间维度
    private DateDimension dateDimension = new DateDimension();

    public ComDimension() {
        super();
    }

    public ComDimension(ContactDimension contactDimension, DateDimension dateDimension) {
        this.contactDimension = contactDimension;
        this.dateDimension = dateDimension;
    }

    public ContactDimension getContactDimension() {
        return contactDimension;
    }

    public void setContactDimension(ContactDimension contactDimension) {
        this.contactDimension = contactDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ComDimension anotherComDimension = (ComDimension) o;
        int result = this.dateDimension.compareTo(anotherComDimension.dateDimension);
        if (result != 0) {
            return result;
        }
        result = this.contactDimension.compareTo(anotherComDimension.contactDimension);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        contactDimension.write(dataOutput);
        dateDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        contactDimension.readFields(dataInput);
        dateDimension.readFields(dataInput);
    }
}

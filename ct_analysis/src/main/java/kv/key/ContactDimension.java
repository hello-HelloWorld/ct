package kv.key;

/*
 * @author: sunxiaoxiong
 * @date  : Created in 2020/4/20 13:57
 */

import kv.base.BaseDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//联系人维度，用于对比联系人
public class ContactDimension extends BaseDimension {
    //联系人电话
    private String telephone;
    //联系人名称
    private String name;

    public ContactDimension() {
        super();
    }

    public ContactDimension(String telephone, String name) {
        this.telephone = telephone;
        this.name = name;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public int hashCode() {
        int result = telephone != null ? telephone.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        ContactDimension other = (ContactDimension) obj;
        if (telephone != null ? !telephone.equals(other.telephone) : other.telephone != null) {
            return false;
        }
        return name != null ? name.equals(other.name) : other.name == null;
    }

    @Override
    public int compareTo(BaseDimension o) {
        ContactDimension anotherContactDimension = (ContactDimension) o;
        int result = this.name.compareTo(anotherContactDimension.name);
        if (result != 0) {
            return result;
        }
        result = this.telephone.compareTo(anotherContactDimension.telephone);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeUTF(this.telephone);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.telephone = dataInput.readUTF();
    }
}

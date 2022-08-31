import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
  * Function to create datatype that has a stores floor space and total net paid.
  *
  */
public class StoreInfo implements WritableComparable<StoreInfo>{
    public double floor_space;
    public double net_paid;


    /**
     * Creates an object of this class
     * @param floor_space the floor space of the store
     * @param net_paid the net paid of the store
     */
    public StoreInfo(double floor_space, double net_paid){
        this.floor_space = floor_space;
        this.net_paid = net_paid;
    }

    /**
     * Creates an empty object of this class
     */
    public StoreInfo(){
        this.floor_space = 0.0f;
        this.net_paid = 0.0f;
    }

    /**
     * Write serialized data
     * 
     * @param out serialized data
     */
    public void write(DataOutput out) throws IOException{
        out.writeDouble(floor_space);
        out.writeDouble(net_paid);
    }

    /**
     * Read serialized data
     * 
     * @param in serialized data
     */
    public void readFields(DataInput in) throws IOException{
        floor_space = in.readDouble();
        net_paid = in.readDouble();
    }

    /**
     * Compares objects of same class
     * 
     * @param another object to compare to
     */
    public int compareTo(StoreInfo otherObj){
         int fscompare = Double.compare(this.floor_space, otherObj.floor_space);

         //if floor space is the same then settle tie by comapring net paid
         if (fscompare == 0){
             return Double.compare(this.net_paid, otherObj.net_paid);
         }
         return fscompare;
    }

    /**
     * Checks if two objects are the same
     * 
     * @param o object to check equality for
     */
    public boolean equals(Object o){
        if(!(o instanceof StoreInfo))
            return false;
        StoreInfo other = (StoreInfo)o;
        return (this.floor_space == other.floor_space) && (this.net_paid == other.net_paid);
    }

    /**
     * Partions obeject data based on hash
     * 
     * @return hashcode of object
     */
    public int hashCode(){
        return (int)Double.doubleToLongBits(this.floor_space)^(int)Double.doubleToLongBits(this.net_paid);
    }
}

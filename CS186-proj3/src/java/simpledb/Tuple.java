package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

	public TupleDesc description = null; //means of tracking the TupleDesc being passed when constructing a tuple
	public RecordId recordId = null; 
	public ArrayList<Field> tuparr = new ArrayList<Field>(); //the meat of the functionality will orbit this class variable... arraylists are easy to extend and access with get, add, set, etc.
    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.description = td;
        int fieldNum = td.numFields();
        this.tuparr = new ArrayList<Field>(fieldNum);
        
        //System.out.print( this.tuparr.size());
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.description;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        //System.out.println(this.tuparr + " " + this.tuparr.size() + " " + i);
        if(i >= 0 && i < this.tuparr.size())
        	this.tuparr.set(i,f);
        else
        	this.tuparr.add(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        //System.out.print(tuparr);
        if (i < this.tuparr.size())
        	return this.tuparr.get(i);
        else
        	return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        Iterator<Field> fieldIt = this.fields();
        String outString = "";
        while(fieldIt.hasNext()) {
        	outString = outString + fieldIt.next().toString() +"\t";
        }
        outString = outString + "\n";
        return outString;
        //throw new UnsupportedOperationException("Implement this");
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return this.tuparr.iterator();
    }
}

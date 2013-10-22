package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	public ArrayList<TDItem> TDArr = new ArrayList<TDItem>(); // will hold the TDItems and easy to manipulate through get, set, add, etc.
	//private static int TDLen = 0;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        Iterator<TDItem> TDIter = this.TDArr.iterator();
        return TDIter;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
   		//this.TDLen = typeAr.length;
   		//System.out.println("print typeAR " + Arrays.toString(typeAr));
   		//System.out.println("printing fieldAr " + Arrays.toString(fieldAr));
   		int typeLen = typeAr.length;
   		this.TDArr = new ArrayList<TDItem>(typeLen);
   		//this.TDArr = new TDItem[this.TDLen];
   		for(int i = 0; i < typeAr.length; i++) {
   			if (fieldAr != null) {
   				this.TDArr.add(new TDItem(typeAr[i],fieldAr[i]));
   			}
   			else {
   				this.TDArr.add(new TDItem(typeAr[i], null));
   			}
   			
   		}
   		//System.out.println("printing the array " + this.TDArr);
   		//this.TDLen = this.TDArr.size();
        
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        //this.TDLen = typeAr.length;
   		TDItem newItem;
   		//this.TDArr = new TDItem[this.TDLen];
   		for(int i = 0; i < typeAr.length; i++) {
   			newItem = new TDItem(typeAr[i], "");
   			this.TDArr.add(newItem);
   		}
        //this.TDLen = this.TDArr.size();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.TDArr.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        TDItem wantedTD = this.TDArr.get(i);
        return wantedTD.fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i >= this.TDArr.size())
        	throw new NoSuchElementException();
        else {
        	TDItem wantedTD = this.TDArr.get(i);
        	//System.out.println(wantedTD + "wantedTD");
        	//System.out.println(wantedTD.fieldType);
        	return wantedTD.fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        Iterator<TDItem> newiter = this.iterator();
        int count = 0;
        String checkName;
        while(newiter.hasNext()) {
        	
        	checkName = newiter.next().fieldName;
        	//System.out.println(checkName + " vs " + name);
        	if (checkName != null && checkName.equals(name)) 
        		return count;
        	else if(checkName != null)
        		count++;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int sizeSum = 0;
        //System.out.println(this.TDArr);
        Iterator<TDItem> newiter = this.iterator();
        while(newiter.hasNext()) {
        	sizeSum += newiter.next().fieldType.getLen();
        }
        return sizeSum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        //int newTDLen = td1.length + td2.length;
        int i = 0;
        //Type[] retFieldType = new Type[newTDLen];
        //String[] retFieldName = new String[newTDlen];
        TDItem newItem;
        TDItem currItem;
        Iterator<TDItem> td1It = td1.iterator();
        Iterator<TDItem> td2It = td2.iterator();
        TupleDesc retTD = new TupleDesc(new Type[]{Type.INT_TYPE});
        while(td1It.hasNext()) {
        	currItem = td1It.next();
        	newItem = new TDItem(currItem.fieldType, currItem.fieldName);
        	retTD.TDArr.add(newItem);
        }
        while(td2It.hasNext()) {
        	currItem = td2It.next();
        	newItem = new TDItem(currItem.fieldType, currItem.fieldName);
        	retTD.TDArr.add(newItem);
        }
        retTD.TDArr.remove(0);
        return retTD;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
     
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)) 
        	return false;
        if (this.TDArr.size() != ((TupleDesc)o).TDArr.size())
        	return false;
        //System.out.println(this.TDArr + " vs " + o);
        for(int i = 0; i < this.TDArr.size(); i++) {
        	//System.out.println("comparing at " + i);
        	if (!this.getFieldType(i).equals(((TupleDesc)o).getFieldType(i))){
        		
        		return false;
        	}
        }
        
        return true;
        	
    }
	
	
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String outputString = "";
        String outputSeg = "";
        TDItem currItem;
        Iterator<TDItem> newiter = this.iterator();
        while(newiter.hasNext()) {
        	currItem = newiter.next();
        	outputSeg = currItem.fieldType.toString() + "(" + currItem.fieldName + "), ";
        	outputString = outputString + outputSeg; 
        }
        return outputString;
    }
}

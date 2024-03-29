package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
	private DbIterator dbi;
	private int aggfield;
	private int groupfield;
	private Aggregator.Op operator;
	private Aggregator aggregate;
	private DbIterator aggIt;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.dbi = child;
        this.aggfield = afield;
        this.groupfield = gfield;
        this.operator = aop;
        //creating the aggregators for either int or string types
        if (this.dbi.getTupleDesc().getFieldType(this.aggfield) == Type.INT_TYPE) {
            if (this.groupfield != -1) {
                aggregate = new IntegerAggregator(this.groupfield, this.dbi.getTupleDesc().getFieldType(this.groupfield), this.aggfield, this.operator);
            }
            else {
                aggregate = new IntegerAggregator(this.groupfield, null, this.aggfield, this.operator);
            }
        } 
        else{
            if (this.groupfield != -1) {
                aggregate = new StringAggregator(this.groupfield, this.dbi.getTupleDesc().getFieldType(this.groupfield), this.aggfield, this.operator);
            }
            else {
                aggregate = new StringAggregator(this.groupfield, null, this.aggfield, this.operator);
            }
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		// some code goes here
		return this.groupfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
		// some code goes here
		if(this.groupfield == -1) {
			return null;
		}
		else {
			return this.dbi.getTupleDesc().getFieldName(this.groupfield);
		}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		// some code goes here
		return this.aggfield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		// some code goes here
		return this.dbi.getTupleDesc().getFieldName(this.aggfield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		// some code goes here
		return this.operator;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		// some code goes here
		super.open();
		this.dbi.open();
		while (this.dbi.hasNext()) {
          	this.aggregate.mergeTupleIntoGroup(this.dbi.next());
        }
        this.aggIt = aggregate.iterator();
        this.aggIt.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if(aggIt.hasNext()) {
			return aggIt.next();
		}
		else {
			return null;
		}
    }

    public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		//super.rewind();
		this.dbi.rewind();
		aggIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
		// some code goes here
		//List<Tuple> myList = new ArrayList<Tuple> ();
        TupleDesc myTD;
        Type[] tdType;
        String[] tdString;
        if(this.aggfield == -1){
        	tdType = new Type[] {this.dbi.getTupleDesc().getFieldType(this.aggfield)};
        	String aggcol = this.operator.toString() + "(" + dbi.getTupleDesc().getFieldName(this.aggfield) + ")";
        	tdString = new String[] {aggcol};
        	myTD = new TupleDesc(tdType,tdString);
            return myTD;     
        } 
        else{
        	tdType = new Type[] {this.dbi.getTupleDesc().getFieldType(this.groupfield), this.dbi.getTupleDesc().getFieldType(this.aggfield)};
        	String aggcol = this.operator.toString() + "(" + dbi.getTupleDesc().getFieldName(this.aggfield) + ")";
        	tdString = new String[] {this.dbi.getTupleDesc().getFieldName(this.groupfield), aggcol};
			myTD = new TupleDesc(tdType, tdString);
            return myTD;    
        }
    }

    public void close() {
	// some code goes here
		super.close();
		this.dbi.close();
		aggIt.close();
    }

    @Override
    public DbIterator[] getChildren() {
		// some code goes here
		return new DbIterator[] {this.dbi};
    }

    @Override
    public void setChildren(DbIterator[] children) {
		// some code goes here
		this.dbi = children[0];
    }
    
}

package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
     
    public HashMap<String, DbFile> tableFile;
    public HashMap<String, String> tablepkey;
    public HashMap<Integer, String> fileId;
    public Catalog() {
        // some code goes here
        tableFile = new HashMap<String, DbFile>();
        tablepkey = new HashMap<String, String>();
        fileId = new HashMap<Integer, String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) throws IllegalArgumentException{
        // some code goes here
        if (file == null || name == null) {
        	throw new IllegalArgumentException();
        }
        else {
        	//String[] tableAdd = {name, pkeyField};
        	tableFile.put(name, file);
        	tablepkey.put(name, pkeyField);
        	fileId.put(file.getId(), name);
        }
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        DbFile tableID = tableFile.get(name);
        if (tableID == null)
        	throw new NoSuchElementException();
        //System.out.println(tableID.getId());
        else 
        	return tableID.getId();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        String fileName = fileId.get(tableid);
        if (fileName.equals(null))
        	throw new NoSuchElementException();
        DbFile retTD = tableFile.get(fileName);
        if (retTD.equals(null))
        	throw new NoSuchElementException();
        //System.out.println(retTD);
        else
        	return retTD.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        String fileName = fileId.get(tableid);
        if (fileName.equals(null))
        	throw new NoSuchElementException();
        DbFile retFile = tableFile.get(fileName);
        if (retFile.equals(null))
        	throw new NoSuchElementException();
        else
	        return retFile;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        String key = tablepkey.get(fileId.get(tableid));
        return key;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        List<Integer> tableIDList = new ArrayList<Integer>();

        for (Integer key : fileId.keySet()) {
            tableIDList.add(key);
        }

        Iterator<Integer> it = tableIDList.iterator();
        return it;
    }

    public String getTableName(int id) {
        // some code goes here
        String name = fileId.get(id);
        return name;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
    	tableFile.clear();
    	tablepkey.clear();
        // some code goes here
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(catalogFile).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder, name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}


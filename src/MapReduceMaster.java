
public class MapReduceMaster {

    private static final String slaveconfigfile = "slaves";
    
    public static void main(String[] args) {
	/* 
	 *  1. Check the standard input.
	 *  Format: java MapReduceMaster <config file> <input file>  
	 */
	if (args.length != 2) {
	    System.out.println("Wrong Argument Number!!  java MapReduceMaster <config file> <input file>");
	    return;
	}
	
	String configfname = args[0];
	String inputfname = args[1];
	
	/* 2. Read in configuration file of this job */
	
	
	
	
	
    }
    
    
    
}

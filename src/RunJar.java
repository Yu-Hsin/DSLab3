import java.io.IOException;


public class RunJar {
    public static void main (String [] args) {
	Runtime rt = Runtime.getRuntime();
	try {
	    Process pr = rt.exec("echo aaaaa");
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }
}

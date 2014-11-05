import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class Output {
    private BufferedWriter [] bw;
    private int numReducer;
    
    public Output (int numReducer, String name) {
	this.numReducer = numReducer;
	bw = new BufferedWriter[numReducer];
	for (int i = 0; i < numReducer; i++) {
	    try {
		bw[i] = new BufferedWriter(new FileWriter(name + i)); //name0, name1, name2, name3...
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }
    
    public void write (String key, String value) {
	int index = key.hashCode() % numReducer;
	try {
	    bw[index].write(key + "\t" + value + "\n");
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
    public void close () {
	for (int i = 0; i < numReducer; i++) {
	    try {
		bw[i].close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }
}

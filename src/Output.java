import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class Output {
    private BufferedWriter [] bw;
    private int numMachine;
    
    public Output (int num, String name) {
	numMachine = num;
	bw = new BufferedWriter[num];
	for (int i = 0; i < num; i++) {
	    try {
		bw[i] = new BufferedWriter(new FileWriter(name + i)); //name0, name1, name2, name3...
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }
    
    //write key-value pair
    public void write (String key, String value) {
	int index = numMachine > 1? Math.abs(key.hashCode()) % numMachine : 0;
	try {
	    bw[index].write(key + "\t" + value + "\n");
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }
    
    public void close () {
	for (int i = 0; i < numMachine; i++) {
	    try {
		bw[i].close();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	}
    }
}

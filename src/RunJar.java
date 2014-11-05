import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;


public class RunJar {
    public static void main (String [] args) throws Exception {
	
	/*Process pro = Runtime.getRuntime().exec("javac HelloWorld.java");
	pro.waitFor();
	pro = Runtime.getRuntime().exec("echo aaaaa");
	pro.waitFor();
	Class<?> myClass = Class.forName("HelloWorld");
	Constructor<?> myCons = myClass.getConstructor();
	Object object = myCons.newInstance();
	Method method = null;
	method = object.getClass().getMethod("mapper");
	method.invoke(object);
	
	
	try {  
	    Process p = Runtime.getRuntime().exec("ls -l");  
	    BufferedReader in = new BufferedReader(  
	                                    new InputStreamReader(p.getInputStream()));  
	    String line = null;  
	    while ((line = in.readLine()) != null) {  
	                    System.out.println(line);  
	    }  
	    } catch (IOException e) {  
	    e.printStackTrace();  
	    }*/  
	node abc = new node (2);
	HashMap <node, Integer> map = new HashMap <node, Integer>();
	System.out.println(abc.hashCode());
	map.put(abc, 3);
	System.out.println(map.get(abc));
	abc.a = 300;
	System.out.println(abc.hashCode());
	System.out.println(map.get(abc));
	
	
    }
    public static class node {
	public int a;
	public node (int a) {
	    this.a = a;
	}
	
	@Override
	public int hashCode () {
	    return a;
	}
    }
}

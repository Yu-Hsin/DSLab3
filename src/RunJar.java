import java.lang.reflect.Constructor;
import java.lang.reflect.Method;


public class RunJar {
    public static void main (String [] args) throws Exception {
	Process pro = Runtime.getRuntime().exec("javac HelloWorld.java");
	pro.waitFor();
	pro = Runtime.getRuntime().exec("echo aaaaa");
	pro.waitFor();
	Class<?> myClass = Class.forName("HelloWorld");
	Constructor<?> myCons = myClass.getConstructor();
	Object object = myCons.newInstance();
	Method method = null;
	method = object.getClass().getMethod("mapper");
	method.invoke(object);
    }
}

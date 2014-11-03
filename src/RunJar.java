import java.lang.reflect.Constructor;
import java.lang.reflect.Method;


public class RunJar {
    public static void main (String [] args) throws Exception {
	Class<?> myClass = Class.forName("HelloWorld");
	Constructor<?> myCons = myClass.getConstructor();
	Object object = myCons.newInstance();
	Method method = null;
	method = object.getClass().getMethod("mapper");
	method.invoke(object);
    }
}

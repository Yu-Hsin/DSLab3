
public class TestMapper implements Mapper{

    @Override
    public void map(String value, Output output) {
	output.write(value, "1");
    }

}

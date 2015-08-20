import io.dbmaster.testng.BaseToolTestNGCase;

import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test

import com.branegy.tools.api.ExportType;


public class ViewTesterIT extends BaseToolTestNGCase {

    @Test
    public void test() {
        def parameters = [ "p_database"  :  getTestProperty("view-tester.p_database")]
        String result = tools.toolExecutor("view-tester", parameters).execute()
        assertTrue(result.contains("View Name"), "Unexpected search results ${result}");
    }
}

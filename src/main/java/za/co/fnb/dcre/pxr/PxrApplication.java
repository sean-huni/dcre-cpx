package za.co.fnb.dcre.pxr;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import za.co.fnb.dcre.platform.batch.ExitCodeMain;
import za.co.fnb.dcre.platform.persistence.JdbcConfig;

@SpringBootApplication
@Import(JdbcConfig.class)
public class PxrApplication {

    public static void main(String[] args) {
        ExitCodeMain.run(PxrApplication.class, args);
    }
}

package za.co.fnb.dcre.pxr.data.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SCRUM-58: response_file widens 128 -> 512 to match
 * file_arrival.physical_filename(512). Before the widening a 129+ char reply
 * name registered in agt_ops, then crashed the reader insert mid-flow
 * (spec 1.4, verified 001-pxr.xml:13). The replay guard
 * UNIQUE (response_file, e2e) must survive the widening untouched.
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false", "dcre.exchange-root=build/test-exchange"})
class ResponseFileWidthIT {

    static final CockroachContainer CRDB =
            new CockroachContainer(DockerImageName.parse("cockroachdb/cockroach:v26.2.3"));

    static {
        CRDB.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", CRDB::getJdbcUrl);
        registry.add("spring.datasource.username", CRDB::getUsername);
        registry.add("spring.datasource.password", CRDB::getPassword);
    }

    static final String RAW_INSERT = "INSERT INTO pbsr_resp (id, response_file, orgnl_msg_id, e2e, status)"
            + " VALUES (gen_random_uuid(), ?, ?, ?, ?)";

    @Autowired
    PbsrRespRepo repo;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void twoHundredCharResponseFileRoundTrips() {
        final String name = "20260716_FNB_PBSR_%s_RESP.xml".formatted("x".repeat(173));
        assertEquals(200, name.length(), "fixture guards the scenario width");

        repo.upsert(name, "MSG-W1", null, "E2E-W1", "ACSC", null);

        assertEquals(name, jdbc.queryForObject(
                        "SELECT response_file FROM pbsr_resp WHERE response_file=? AND e2e='E2E-W1'",
                        String.class, name),
                "a 200-char reply name must round-trip byte-exact through pbsr_resp");
    }

    @Test
    void replayGuardUniqueSurvivesWidening() {
        final String name = "20260716_FNB_PBSR_guard_RESP.xml";
        jdbc.update(RAW_INSERT, name, "MSG-W2", "E2E-W2", "ACSC");

        assertThrows(DuplicateKeyException.class,
                () -> jdbc.update(RAW_INSERT, name, "MSG-W2", "E2E-W2", "ACSC"),
                "UNIQUE (response_file, e2e) must still arbitrate raw replays");

        repo.upsert(name, "MSG-W2", null, "E2E-W2", "RJCT", "AC04");
        assertEquals(1, repo.countByResponseFile(name), "repo replay stays an ON CONFLICT no-op");
        assertEquals("RJCT", jdbc.queryForObject(
                "SELECT status FROM pbsr_resp WHERE response_file=? AND e2e='E2E-W2'", String.class, name));
    }
}

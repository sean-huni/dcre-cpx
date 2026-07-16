package za.co.fnb.dcre.pxr.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.utility.DockerImageName;
import za.co.fnb.dcre.pxr.CrwSourceTables;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-55 Task 8: PXR resolves OrgnlMsgId to the emitting CRW batch.
 * Membership is fail-closed (a foreign e2e is never ingested) while ingest
 * itself is fail-open when the emission registry has no row for the
 * OrgnlMsgId: statuses are still truth even if CRW's registry is behind.
 * A partial PBSR persists what it carries immediately; missing members stay
 * pending with NO synthetic rows (spec test 5, Sean ruling 18).
 */
@SpringBootTest(properties = {"spring.batch.job.enabled=false"})
class BatchCorrelationIT {

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

    @Autowired
    ReaderService service;

    @Autowired
    JdbcTemplate jdbc;

    ListAppender<ILoggingEvent> logs;
    String responseFile;

    @BeforeEach
    void setUp() {
        CrwSourceTables.ensure(jdbc);
        responseFile = "20260716_FNB_PBSR_%s_RESP.xml"
                .formatted(UUID.randomUUID().toString().substring(0, 8));
        logs = new ListAppender<>();
        logs.start();
        ((Logger) LoggerFactory.getLogger(ReaderService.class)).addAppender(logs);
    }

    @AfterEach
    void detachAppender() {
        ((Logger) LoggerFactory.getLogger(ReaderService.class)).detachAppender(logs);
    }

    @Test
    void repliesCorrelateToBatchAndForeignE2eIsFailClosed() {
        UUID emissionId = seedEmission("DCRERF2026071600000021_2", 2, "E2E-A", "E2E-B", "E2E-C");
        String reply = reply("DCRERF2026071600000021_2",
                tx("E2E-A", "ACSC", null), tx("E2E-B", "RJCT", "AC04"), tx("E2E-Z", "ACSC", null));

        service.ingest(reply, responseFile);

        assertEquals(2, rowCount(), "only member verdicts persist");
        assertEquals(emissionId, emissionIdOf("E2E-A"));
        assertEquals(emissionId, emissionIdOf("E2E-B"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM pbsr_resp WHERE response_file=?"
                        + " AND e2e='E2E-Z'", Integer.class, responseFile),
                "foreign e2e is fail-closed, never ingested");
        assertTrue(warns().stream().anyMatch(m -> m.contains("stage=PXR")
                        && m.contains("e2e=E2E-Z") && m.contains("reason=FOREIGN_E2E")),
                "foreign e2e exclusion must WARN, got: " + warns());
    }

    @Test
    void unknownOutboundMsgIngestsFailOpenWithNullEmission() {
        String reply = reply("DCRERF2026071699999999",
                tx("E2E-U1", "ACSC", null), tx("E2E-U2", "RJCT", "AC04"));

        service.ingest(reply, responseFile);

        assertEquals(2, rowCount(), "fail-open ingest: statuses land even when the registry is behind");
        assertNull(emissionIdOf("E2E-U1"), "no emission resolved, correlation stays NULL");
        assertEquals(1, warns().stream().filter(m -> m.contains("reason=UNKNOWN_OUTBOUND_MSG")).count(),
                "single file-level WARN for the unknown OrgnlMsgId, got: " + warns());
        assertTrue(warns().stream().noneMatch(m -> m.contains("reason=FOREIGN_E2E")),
                "no membership check without a resolved batch");
    }

    @Test
    void partialPbsrPersistsImmediatelyAndMintsNoSyntheticSiblings() {
        UUID emissionId = seedEmission("DCRERF2026071600000022_1", 1, "E2E-A", "E2E-B", "E2E-C");
        String reply = reply("DCRERF2026071600000022_1", tx("E2E-A", "ACSC", null));

        service.ingest(reply, responseFile);

        assertEquals(1, rowCount(), "the carried verdict persists immediately");
        assertEquals(emissionId, emissionIdOf("E2E-A"));
        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM pbsr_resp WHERE emission_id=?"
                        + " AND e2e IN ('E2E-B','E2E-C')", Integer.class, emissionId),
                "missing members stay pending, no synthetic rows minted");
    }

    private UUID seedEmission(final String outboundMsgId, final int ordinal, final String... memberE2e) {
        UUID id = UUID.randomUUID();
        // Column shapes: crw 001-crw.xml + 003-split.xml (SCRUM-55-feat-batch-split).
        jdbc.update("INSERT INTO crw_emission (id, arrival_id, run_date, file_name, state,"
                        + " batch_ordinal, outbound_msg_id, tx_count, control_sum)"
                        + " VALUES (?,?,?,?,?,?,?,?,?)",
                id, UUID.randomUUID(), Date.valueOf(LocalDate.of(2026, 7, 16)),
                "FNBRF01_%s_PAIN008.xml".formatted(outboundMsgId), "VISIBLE",
                ordinal, outboundMsgId, (long) memberE2e.length,
                new BigDecimal("10.00").multiply(BigDecimal.valueOf(memberE2e.length)));
        int sequence = 1;
        for (final String e2e : memberE2e) {
            jdbc.update("INSERT INTO crw_emission_member (id, emission_id, sequence, e2e, amount)"
                            + " VALUES (?,?,?,?,?)",
                    UUID.randomUUID(), id, sequence++, e2e, new BigDecimal("10.00"));
        }
        return id;
    }

    private UUID emissionIdOf(final String e2e) {
        return jdbc.queryForObject("SELECT emission_id FROM pbsr_resp WHERE response_file=? AND e2e=?",
                UUID.class, responseFile, e2e);
    }

    private int rowCount() {
        return jdbc.queryForObject("SELECT count(*) FROM pbsr_resp WHERE response_file=?",
                Integer.class, responseFile);
    }

    private List<String> warns() {
        return logs.list.stream().filter(event -> event.getLevel() == Level.WARN)
                .map(ILoggingEvent::getFormattedMessage).toList();
    }

    // [SYNTHETIC-CONTRACT R-35] reply shape
    private static String reply(final String orgnlMsgId, final String... txBlocks) {
        StringBuilder xml = new StringBuilder("<Document>\n  <OrgnlMsgId>")
                .append(orgnlMsgId).append("</OrgnlMsgId>\n");
        for (final String block : txBlocks) {
            xml.append("  ").append(block).append('\n');
        }
        return xml.append("</Document>\n").toString();
    }

    private static String tx(final String e2e, final String status, final String reason) {
        return "<Tx><OrgnlEndToEndId>%s</OrgnlEndToEndId><TxSts>%s</TxSts>%s</Tx>"
                .formatted(e2e, status, reason == null ? "" : "<Rsn>%s</Rsn>".formatted(reason));
    }
}

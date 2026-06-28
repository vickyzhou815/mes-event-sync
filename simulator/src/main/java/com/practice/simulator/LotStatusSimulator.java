package com.practice.simulator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class LotStatusSimulator {

    private static final String[] STATES = {"started", "in_process", "complete", "hold", "scrap"};
    private static final int NORMAL_INTERVAL_MS = 2000;
    private static final int BURST_INTERVAL_MS = 50;
    private static final int BURST_DURATION_SEC = 20;
    private static final int LOT_POOL_SIZE = 200;

    private final Random random = new Random();
    private final List<String> activeLotIds = new ArrayList<>();
    private final String jdbcUrl;
    private final String user;
    private final String password;

    public LotStatusSimulator(String jdbcUrl, String user, String password) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        for (int i = 0; i < LOT_POOL_SIZE; i++) {
            activeLotIds.add(String.format("LOT-%05d", i));
        }
    }

    public void run() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password)) {
            System.out.println("Connected to MySQL. Starting lot state simulation...");

            long startTime = System.currentTimeMillis();
            long nextBurstAt = startTime + 15_000;
            boolean inBurst = false;
            long burstEndsAt = 0;

            while (true) {
                long now = System.currentTimeMillis();

                if (!inBurst && now >= nextBurstAt) {
                    inBurst = true;
                    burstEndsAt = now + (BURST_DURATION_SEC * 1000L);
                    System.out.println(">>> BURST: simulating end-of-batch cascade <<<");
                }
                if (inBurst && now >= burstEndsAt) {
                    inBurst = false;
                    nextBurstAt = now + 45_000;
                    System.out.println("<<< burst ended, back to steady state >>>");
                }

                String lotId = activeLotIds.get(random.nextInt(activeLotIds.size()));
                String state = STATES[random.nextInt(STATES.length)];
                String toolId = String.format("TOOL-%02d", random.nextInt(20));

                upsertLotStatus(conn, lotId, state, toolId);

                Thread.sleep(inBurst ? BURST_INTERVAL_MS : NORMAL_INTERVAL_MS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void upsertLotStatus(Connection conn, String lotId, String state, String toolId) {
        String sql = "INSERT INTO lot_status (lot_id, state, tool_id) VALUES (?, ?, ?) " +
                      "ON DUPLICATE KEY UPDATE state = ?, tool_id = ?";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, lotId);
            stmt.setString(2, state);
            stmt.setString(3, toolId);
            stmt.setString(4, state);
            stmt.setString(5, toolId);
            stmt.executeUpdate();
            System.out.println("lot=" + lotId + " -> state=" + state + " tool=" + toolId);
        } catch (SQLException e) {
            System.err.println("Failed to update lot status: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws SQLException {
        String jdbcUrl = args.length > 0 ? args[0] : "jdbc:mysql://localhost:3306/mes_db";
        String user = args.length > 1 ? args[1] : "root";
        String password = args.length > 2 ? args[2] : "practice";
        new LotStatusSimulator(jdbcUrl, user, password).run();
    }
}


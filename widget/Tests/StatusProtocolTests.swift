import XCTest

final class StatusProtocolTests: XCTestCase {
    func testCurrentProtocolDecodesExactPoolOutlook() throws {
        let status = try decode(
            """
            {
              "proto": 1,
              "version": "0.60.0",
              "generated_at": "2026-07-19T09:00:00Z",
              "accounts": [],
              "pool": {
                "remaining_5h_pct": 41,
                "remaining_7d_pct": 72,
                "net_burn_5h_per_hour": -2,
                "pace_5h": 0.4,
                "pace_7d": 0.7,
                "mood": "easy"
              }
            }
            """)

        XCTAssertEqual(status.proto, PoolStatus.protocolVersion)
        XCTAssertEqual(status.outlook?.remaining5hPct, 41)
        XCTAssertEqual(status.outlook?.remaining7dPct, 72)
        XCTAssertEqual(status.outlook?.burn5hPerHour, 0)
        XCTAssertEqual(status.outlook?.netBurn5hPerHour, -2)
        XCTAssertEqual(status.outlook?.pace5h, 0.4)
        XCTAssertEqual(status.outlook?.pace7d, 0.7)
        XCTAssertEqual(status.outlook?.mood, .easy)
    }

    func testCurrentProtocolAllowsNoPoolBeforeAnySample() throws {
        let status = try decode(
            """
            {
              "proto": 1,
              "version": "0.60.0",
              "generated_at": "2026-07-19T09:00:00.123Z",
              "accounts": []
            }
            """)

        XCTAssertNil(status.outlook)
    }

    func testSampledAccountWithoutPoolIsRejected() {
        XCTAssertThrowsError(try decode(
            """
            {
              "proto": 1,
              "version": "0.60.0",
              "generated_at": "2026-07-19T09:00:00Z",
              "accounts": [{
                "id": 18,
                "config_dir": "/Users/you/.cc-pool/accounts/acct-18",
                "label": "acct-18",
                "score": 40,
                "remaining_5h": 50,
                "remaining_7d": 70,
                "active_sessions": 0,
                "rate_limited": false,
                "has_usage": true,
                "stale": false,
                "resets_5h": "2026-07-19T10:00:00Z",
                "resets_7d": "2026-07-26T09:00:00Z"
              }]
            }
            """))
    }

    func testPreRewriteProtocolIsRejected() {
        XCTAssertThrowsError(try decode(
            """
            {
              "proto": 2,
              "version": "0.59.0",
              "generated_at": "2026-07-19T09:00:00Z",
              "accounts": []
            }
            """))
    }

    func testMissingRequiredPoolFieldsAreRejected() throws {
        let missingFields = [
            "net_burn_5h_per_hour",
            "pace_5h",
            "pace_7d",
            "mood",
        ]
        for field in missingFields {
            var pool: [String: Any] = [
                "remaining_5h_pct": 41,
                "remaining_7d_pct": 72,
                "net_burn_5h_per_hour": -2,
                "pace_5h": 0.4,
                "pace_7d": 0.7,
                "mood": "easy",
            ]
            pool.removeValue(forKey: field)
            let object: [String: Any] = [
                "proto": 1,
                "version": "0.60.0",
                "generated_at": "2026-07-19T09:00:00Z",
                "accounts": [],
                "pool": pool,
            ]
            let data = try JSONSerialization.data(withJSONObject: object)
            XCTAssertThrowsError(try JSONDecoder.poolStatus.decode(PoolStatus.self, from: data), field)
        }
    }

    func testMalformedPoolIsRejected() {
        XCTAssertThrowsError(try decode(
            """
            {
              "proto": 1,
              "version": "0.60.0",
              "generated_at": "2026-07-19T09:00:00Z",
              "accounts": [],
              "pool": "not-an-outlook"
            }
            """))
    }

    func testCredentialQuarantineIsDecodedAndNeverRankedFirst() throws {
        let status = try decode(
            """
            {
              "proto": 1,
              "version": "0.60.0",
              "generated_at": "2026-07-19T09:00:00Z",
              "accounts": [
                {
                  "id": 1,
                  "config_dir": "/tmp/acct-1",
                  "label": "quarantined",
                  "score": 100,
                  "remaining_5h": 100,
                  "remaining_7d": 100,
                  "active_sessions": 0,
                  "rate_limited": false,
                  "credential_quarantined": true,
                  "has_usage": false,
                  "stale": false,
                  "resets_5h": "0001-01-01T00:00:00Z",
                  "resets_7d": "0001-01-01T00:00:00Z"
                },
                {
                  "id": 2,
                  "config_dir": "/tmp/acct-2",
                  "label": "healthy",
                  "score": 1,
                  "remaining_5h": 100,
                  "remaining_7d": 100,
                  "active_sessions": 0,
                  "rate_limited": false,
                  "has_usage": false,
                  "stale": false,
                  "resets_5h": "0001-01-01T00:00:00Z",
                  "resets_7d": "0001-01-01T00:00:00Z"
                }
              ]
            }
            """)

        XCTAssertTrue(status.accounts[0].credentialQuarantined)
        XCTAssertTrue(status.accounts[0].unusable)
        XCTAssertFalse(status.accounts[1].credentialQuarantined)
        XCTAssertEqual(status.accounts.ranked.first?.id, 2)
        XCTAssertTrue(PoolStatus.sample.accounts.contains(where: \.credentialQuarantined))
    }

    private func decode(_ json: String) throws -> PoolStatus {
        try JSONDecoder.poolStatus.decode(PoolStatus.self, from: Data(json.utf8))
    }
}

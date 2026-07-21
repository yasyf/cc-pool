import XCTest

final class OutlookDisplayTests: XCTestCase {
    private static let iso: ISO8601DateFormatter = {
        let f = ISO8601DateFormatter()
        f.formatOptions = [.withInternetDateTime]
        return f
    }()

    private func decodeAccount(_ overrides: [String: Any]) throws -> AccountStatus {
        var obj: [String: Any] = [
            "id": 1, "config_dir": "/Users/you/.cc-pool/accounts/acct-01",
            "label": "acct", "score": 0,
            "remaining_5h": 50, "remaining_7d": 50, "active_sessions": 0,
            "rate_limited": false, "has_usage": true, "stale": false,
        ]
        obj.merge(overrides) { _, new in new }
        let data = try JSONSerialization.data(withJSONObject: obj)
        return try JSONDecoder.poolStatus.decode(AccountStatus.self, from: data)
    }

    private func decodeOutlook(remaining5h: Double, remaining7d: Double) throws -> PoolOutlook {
        let data = try JSONSerialization.data(withJSONObject: [
            "remaining_5h_pct": remaining5h, "remaining_7d_pct": remaining7d,
        ])
        return try JSONDecoder.poolStatus.decode(PoolOutlook.self, from: data)
    }

    func testWeeklyExhaustedDecode() throws {
        let cases: [(name: String, override: [String: Any], want: Bool)] = [
            ("present true", ["weekly_exhausted": true], true),
            ("present false", ["weekly_exhausted": false], false),
            ("absent reads false (old daemon compat)", [:], false),
        ]
        for c in cases {
            XCTAssertEqual(try decodeAccount(c.override).isWeeklyExhausted, c.want, c.name)
        }
    }

    func testBindingWindowPicker() throws {
        let cases: [(name: String, r5: Double, r7: Double, weekBinds: Bool,
                     bindLabel: String, bindPct: Int, otherLabel: String, otherPct: Int)] = [
            ("7d fuller binds 7d", 92, 34, true, "7d", 66, "5h", 8),
            ("5h fuller binds 5h", 20, 70, false, "5h", 80, "7d", 30),
            ("tie breaks to 5h", 40, 40, false, "5h", 60, "7d", 60),
            ("rounds to whole percents", 91.6, 33.4, true, "7d", 67, "5h", 8),
        ]
        for c in cases {
            let o = try decodeOutlook(remaining5h: c.r5, remaining7d: c.r7)
            XCTAssertEqual(o.weekBinds, c.weekBinds, c.name)
            XCTAssertEqual(o.bindingLabel, c.bindLabel, c.name)
            XCTAssertEqual(o.bindingUsedPct, c.bindPct, c.name)
            XCTAssertEqual(o.otherLabel, c.otherLabel, c.name)
            XCTAssertEqual(o.otherUsedPct, c.otherPct, c.name)
        }
    }

    func testFooterTextShapes() {
        XCTAssertNil(footerText(overflow: 0, exhausted: 0), "empty footer renders nothing")

        let both = footerText(overflow: 12, exhausted: 6)
        XCTAssertEqual(both?.overflow, "+12 more")
        XCTAssertEqual(both?.exhausted, " · 6 exhausted")

        let overflowOnly = footerText(overflow: 12, exhausted: 0)
        XCTAssertEqual(overflowOnly?.overflow, "+12 more")
        XCTAssertNil(overflowOnly?.exhausted, "no exhausted run when the count is 0")
    }

    func testWeeklyExhaustionReset() throws {
        let scopedISO = "2026-07-25T14:00:00Z"
        let agg7dISO = "2026-07-23T09:00:00Z"
        let scopedDate = Self.iso.date(from: scopedISO)!
        let agg7dDate = Self.iso.date(from: agg7dISO)!

        let cases: [(name: String, override: [String: Any], label: String?, reset: Date?)] = [
            ("scoped fuller picks the scoped bucket",
             ["weekly_exhausted": true, "remaining_7d": 20, "scoped_7d_model": "Fable",
              "scoped_7d_util": 100, "scoped_7d_resets": scopedISO, "resets_7d": agg7dISO],
             "Fable", scopedDate),
            ("aggregate fuller falls back to 7d",
             ["weekly_exhausted": true, "remaining_7d": 20, "scoped_7d_model": "Fable",
              "scoped_7d_util": 50, "scoped_7d_resets": scopedISO, "resets_7d": agg7dISO],
             "7d", agg7dDate),
            ("no scoped bucket falls back to 7d",
             ["weekly_exhausted": true, "remaining_7d": 20, "resets_7d": agg7dISO],
             "7d", agg7dDate),
            ("not weekly-exhausted is nil",
             ["weekly_exhausted": false, "remaining_7d": 20, "scoped_7d_model": "Fable",
              "scoped_7d_util": 100, "scoped_7d_resets": scopedISO],
             nil, nil),
            ("scoped chosen but its reset missing is nil (no 7d fallthrough)",
             ["weekly_exhausted": true, "remaining_7d": 20, "scoped_7d_model": "Fable",
              "scoped_7d_util": 100, "resets_7d": agg7dISO],
             nil, nil),
            ("aggregate chosen but 7d reset missing is nil",
             ["weekly_exhausted": true, "remaining_7d": 20, "scoped_7d_model": "Fable",
              "scoped_7d_util": 50],
             nil, nil),
        ]
        for c in cases {
            let got = try decodeAccount(c.override).weeklyExhaustionReset
            if let label = c.label {
                XCTAssertEqual(got?.label, label, c.name)
                XCTAssertEqual(got?.reset, c.reset, c.name)
            } else {
                XCTAssertNil(got, c.name)
            }
        }
    }
}

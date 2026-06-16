package mountd

import (
	"encoding/json"
	"reflect"
	"testing"
)

// These tests pin the proto-1 wire artifacts byte-for-byte: JSON field names,
// op strings, and error-class strings are FROZEN (see the package godoc). Old
// daemons drive new holders and new daemons drive old holders as a matter of
// routine, so ANY change that makes a golden literal here fail is a protocol
// break — do not "fix" the literal; fix the change. New capability means a
// new op or a new optional field with a new name, which extends these tests
// without altering an existing golden.

func TestWireFreezeProtoVersion(t *testing.T) {
	if MountProtoVersion != 1 {
		t.Fatalf("MountProtoVersion = %d; proto-1 is frozen — new capability never bumps it", MountProtoVersion)
	}
}

func TestWireFreezeOps(t *testing.T) {
	want := map[string]Op{
		"health":   OpHealth,
		"probe":    OpProbe,
		"mount":    OpMount,
		"unmount":  OpUnmount,
		"list":     OpList,
		"shutdown": OpShutdown,
	}
	for frozen, op := range want {
		if string(op) != frozen {
			t.Errorf("op drifted: %q, frozen value is %q", op, frozen)
		}
	}
}

func TestWireFreezeErrClasses(t *testing.T) {
	want := map[string]string{
		"tcc":           ClassTCC,
		"mount-timeout": ClassMountTimeout,
		"mount-failed":  ClassMountFailed,
		"wedged":        ClassWedged,
		"foreign-mount": ClassForeignMount,
		"busy":          ClassBusy,
		"base-mismatch": ClassBaseMismatch,
	}
	for frozen, class := range want {
		if class != frozen {
			t.Errorf("error class drifted: %q, frozen value is %q", class, frozen)
		}
	}
}

func TestWireFreezeRequest(t *testing.T) {
	tests := []struct {
		name string
		in   Request
		want string
	}{
		{
			name: "every field set",
			in:   Request{Proto: 1, Op: OpMount, Base: "/pool/base", Dir: "/pool/acct-01"},
			want: `{"proto":1,"op":"mount","base":"/pool/base","dir":"/pool/acct-01"}`,
		},
		{
			name: "optional fields omitted when empty",
			in:   Request{Proto: 1, Op: OpHealth},
			want: `{"proto":1,"op":"health"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-1 wire artifact — see file comment)", got, tc.want)
			}
			var back Request
			if err := json.Unmarshal([]byte(tc.want), &back); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(back, tc.in) {
				t.Fatalf("round-trip = %+v, want %+v", back, tc.in)
			}
		})
	}
}

func TestWireFreezeResponse(t *testing.T) {
	tests := []struct {
		name string
		in   Response
		want string
	}{
		{
			name: "every field set",
			in: Response{
				Proto:    1,
				OK:       false,
				Error:    "boom",
				ErrClass: ClassTCC,
				Version:  "1.2.3 (abc1234)",
				FuseOK:   true,
				Mounts:   []MountInfo{{Dir: "/pool/acct-01", Base: "/pool/base", Live: true}},
			},
			want: `{"proto":1,"ok":false,"error":"boom","err_class":"tcc","version":"1.2.3 (abc1234)","fuse_ok":true,"mounts":[{"dir":"/pool/acct-01","base":"/pool/base","live":true}]}`,
		},
		{
			name: "optional fields omitted when empty",
			in:   Response{Proto: 1, OK: true},
			want: `{"proto":1,"ok":true}`,
		},
		{
			name: "MountInfo always carries all three fields",
			in:   Response{Proto: 1, OK: true, Mounts: []MountInfo{{Dir: "/pool/acct-01", Base: "/pool/base", Live: false}}},
			want: `{"proto":1,"ok":true,"mounts":[{"dir":"/pool/acct-01","base":"/pool/base","live":false}]}`,
		},
		{
			// Additive proto-1 extension: the epoch/mount-time fields ride
			// alongside the frozen trio and vanish when zero (the case above
			// pins that an old-shape MountInfo still marshals to the exact old
			// bytes). The deep verdict is NOT on the wire — the daemon owns the
			// probe, so the holder ships no wedge field.
			name: "MountInfo epoch and mount-time fields are additive",
			in:   Response{Proto: 1, OK: true, Mounts: []MountInfo{{Dir: "/pool/acct-01", Base: "/pool/base", Live: false, Epoch: 3, MountedAt: 1765500000}}},
			want: `{"proto":1,"ok":true,"mounts":[{"dir":"/pool/acct-01","base":"/pool/base","live":false,"epoch":3,"mounted_at":1765500000}]}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-1 wire artifact — see file comment)", got, tc.want)
			}
			var back Response
			if err := json.Unmarshal([]byte(tc.want), &back); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(back, tc.in) {
				t.Fatalf("round-trip = %+v, want %+v", back, tc.in)
			}
		})
	}
}

// TestWireFreezeEmptyMountsOmitted pins the clean-shutdown wire shape: an
// empty (non-nil) failed-dirs slice is omitted entirely, so old drivers see
// exactly the same bytes a holder has always sent for a clean sweep.
func TestWireFreezeEmptyMountsOmitted(t *testing.T) {
	got, err := json.Marshal(Response{Proto: 1, OK: true, Mounts: []MountInfo{}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"proto":1,"ok":true}`; string(got) != want {
		t.Fatalf("marshal = %s, want %s", got, want)
	}
}

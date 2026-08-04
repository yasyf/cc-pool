package daemon

import (
	"io"
	"strings"
	"testing"

	"github.com/yasyf/cc-pool/internal/accountterminal"
)

func TestValidateAccountMutationPollPageRequiresContiguousCursor(t *testing.T) {
	tests := []struct {
		name   string
		page   AccountMutationPollResponse
		cursor uint64
		want   string
	}{
		{
			name:   "empty page holds the cursor",
			page:   AccountMutationPollResponse{NextCursor: 7},
			cursor: 7,
		},
		{
			name: "chunks advance the cursor exactly",
			page: AccountMutationPollResponse{
				Chunks: [][]byte{[]byte("a"), []byte("b")}, NextCursor: 9,
			},
			cursor: 7,
		},
		{
			name:   "a moved cursor with no chunks is a gap",
			page:   AccountMutationPollResponse{NextCursor: 9},
			cursor: 7,
			want:   "does not follow",
		},
		{
			name: "an empty chunk is refused",
			page: AccountMutationPollResponse{
				Chunks: [][]byte{{}}, NextCursor: 8,
			},
			cursor: 7,
			want:   "empty or oversized",
		},
		{
			name: "an oversized chunk is refused",
			page: AccountMutationPollResponse{
				Chunks: [][]byte{make([]byte, accountterminal.TerminalChunkSize+1)}, NextCursor: 8,
			},
			cursor: 7,
			want:   "empty or oversized",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAccountMutationPollPage(tt.page, tt.cursor)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validate = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validate = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestAccountMutationTerminalEndpointDrainsBufferedPageBeforeEOF(t *testing.T) {
	endpoint := &accountMutationTerminalEndpoint{
		next:     3,
		buffered: [][]byte{[]byte("first"), []byte("second")},
		done:     true,
	}
	first, err := endpoint.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if first.Sequence != 3 || string(first.Data) != "first" {
		t.Fatalf("first output = %#v", first)
	}
	second, err := endpoint.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence != 4 || string(second.Data) != "second" {
		t.Fatalf("second output = %#v", second)
	}
	if _, err := endpoint.Receive(t.Context()); err != io.EOF {
		t.Fatalf("drained endpoint Receive = %v, want io.EOF", err)
	}
	if endpoint.next != 5 {
		t.Fatalf("cursor after page = %d, want 5", endpoint.next)
	}
}

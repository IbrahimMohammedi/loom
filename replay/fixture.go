package replay

import (
	"encoding/json"
	"io"

	"github.com/IbrahimMohammedi/loom"
)

// Fixture is the on-disk form of an exported instance log — what the
// inspector's export-as-replay-test button downloads (ADR-12).
type Fixture struct {
	InstanceID string       `json:"instance_id"`
	Workflow   string       `json:"workflow"`
	Status     string       `json:"status"`
	Events     []loom.Event `json:"events"`
}

func WriteFixture(w io.Writer, f Fixture) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(f)
}

func ReadFixture(r io.Reader) (Fixture, error) {
	var f Fixture
	err := json.NewDecoder(r).Decode(&f)
	return f, err
}

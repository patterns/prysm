package p2p

import (
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/prysmaticlabs/prysm/config/params"
	"github.com/prysmaticlabs/prysm/testing/assert"
	"github.com/prysmaticlabs/prysm/testing/require"
	logTest "github.com/sirupsen/logrus/hooks/test"
)

func newLocalListener(t testing.TB) net.Listener {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Helper()
		t.Fatal(err)
	}
	return ln
}

func closeListener(t testing.TB, l net.Listener) {
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// Test `verifyConnectivity` function by trying to connect to google.com (successfully)
// and then by connecting to an unreachable IP and ensuring that a log is emitted
func TestVerifyConnectivity(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	hook := logTest.NewGlobal()
	ln := newLocalListener(t)
	defer closeListener(t, ln)
	lnAd, lnPort, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	testPort, err := strconv.Atoi(lnPort)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		address              string
		port                 uint
		expectedConnectivity bool
		name                 string
	}{
		{lnAd, uint(testPort), true, "Dial reachable IP: " + ln.Addr().String()},
		{"198.18.0.254", 0, false, "Dialing an unreachable IP: 198.18.0.254:0"},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf(tc.name),
			func(t *testing.T) {
				verifyConnectivity(tc.address, tc.port, "tcp")
				logMessage := "IP address is not accessible"
				if tc.expectedConnectivity {
					require.LogsDoNotContain(t, hook, logMessage)
				} else {
					require.LogsContain(t, hook, logMessage)
				}
			})
	}
}

func TestSerializeENR(t *testing.T) {
	params.SetupTestConfigCleanup(t)
	t.Run("Ok", func(t *testing.T) {
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		db, err := enode.OpenDB("")
		require.NoError(t, err)
		lNode := enode.NewLocalNode(db, key)
		record := lNode.Node().Record()
		s, err := SerializeENR(record)
		require.NoError(t, err)
		assert.NotEqual(t, "", s)
	})

	t.Run("Nil record", func(t *testing.T) {
		_, err := SerializeENR(nil)
		require.NotNil(t, err)
		assert.ErrorContains(t, "could not serialize nil record", err)
	})
}

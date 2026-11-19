package gortsplib

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/liberrors"
)

// ServerSlowClientPolicy is the policy applied to reading sessions
// that cannot keep up with the stream.
//
// The policy is evaluated independently for each reading session:
// a slow session never blocks nor slows down other readers of the
// same stream.
//
// The policy applies to sessions that read with the UDP (unicast) or
// TCP transports, since they own a dedicated outbound queue.
// It does not apply to the UDP-multicast transport, in which all readers
// share a single outbound queue, nor to recording sessions.
type ServerSlowClientPolicy int

const (
	// ServerSlowClientPolicyNone preserves the default behavior:
	// when the outbound queue of a session is full, packets are refused
	// and an error is reported to the stream writer.
	ServerSlowClientPolicyNone ServerSlowClientPolicy = iota

	// ServerSlowClientPolicyDropStale drops stale, non-key video packets
	// when the limits set with SlowClientMaxQueueBytes or
	// SlowClientMaxQueueAge are exceeded.
	// Keyframes, codec parameter sets, audio and RTCP packets are never
	// dropped: for them, SlowClientMaxQueueBytes acts as a soft limit,
	// while the fixed-size write queue (WriteQueueSize) remains a hard
	// limit, exactly as with ServerSlowClientPolicyNone.
	ServerSlowClientPolicyDropStale

	// ServerSlowClientPolicyDisconnect closes reading sessions that
	// exceed SlowClientMaxQueueBytes, SlowClientMaxQueueAge or
	// WriteQueueSize. The session is closed with a
	// liberrors.ErrServerSlowClient error, that is passed to
	// ServerHandlerOnSessionClose.
	ServerSlowClientPolicyDisconnect
)

// String implements fmt.Stringer.
func (p ServerSlowClientPolicy) String() string {
	switch p {
	case ServerSlowClientPolicyNone:
		return "none"
	case ServerSlowClientPolicyDropStale:
		return "dropStale"
	case ServerSlowClientPolicyDisconnect:
		return "disconnect"
	}
	return "unknown"
}

// isDroppablePacket returns true when pkt can be safely discarded
// without preventing the decoding of subsequent packets.
func isDroppablePacket(medi *description.Media, forma format.Format, pkt *rtp.Packet) bool {
	if medi.Type != description.MediaTypeVideo {
		return false
	}

	switch forma.(type) {
	case *format.H264, *format.H265:
		// PTSEqualsDTS returns true for IDR frames and parameter sets
		// (VPS/SPS/PPS), that are necessary to decode subsequent packets.
		return !forma.PTSEqualsDTS(pkt)

	default:
		// keyframes of other codecs cannot be detected without
		// fully decoding the payload: never drop.
		return false
	}
}

// serverSessionQueueEntry is an entry of the outbound queue of a session.
type serverSessionQueueEntry struct {
	size       int
	enqueuedAt time.Time
	droppable  bool
	run        func() error
}

// serverSessionBackpressure implements the slow client policy
// of a single reading session.
type serverSessionBackpressure struct {
	ss *ServerSession

	queuedPackets  atomic.Int64
	queuedBytes    atomic.Int64
	droppedPackets atomic.Uint64
	droppedBytes   atomic.Uint64
}

func (bp *serverSessionBackpressure) drop(entry *serverSessionQueueEntry) {
	bp.droppedPackets.Add(1)
	bp.droppedBytes.Add(uint64(entry.size))
}

func (bp *serverSessionBackpressure) disconnect(reason string) error {
	err := liberrors.ErrServerSlowClient{Reason: reason}
	bp.ss.closeWithError(err)
	return err
}

// push adds an entry to the outbound queue, applying the slow client policy.
// It must be called with writerMutex held (at least for reading).
func (bp *serverSessionBackpressure) push(entry *serverSessionQueueEntry) error {
	s := bp.ss.s

	if maxBytes := int64(s.SlowClientMaxQueueBytes); maxBytes > 0 &&
		bp.queuedBytes.Load()+int64(entry.size) > maxBytes {
		if s.SlowClientPolicy == ServerSlowClientPolicyDisconnect {
			return bp.disconnect(fmt.Sprintf("queued bytes exceeded limit of %d", maxBytes))
		}

		if entry.droppable {
			bp.drop(entry)
			return nil
		}

		// keyframes, parameter sets, audio and RTCP packets are never
		// dropped: the byte limit is soft for them.
	}

	bp.queuedPackets.Add(1)
	bp.queuedBytes.Add(int64(entry.size))

	ok := bp.ss.writer.Push(func() error {
		bp.queuedPackets.Add(-1)
		bp.queuedBytes.Add(int64(-entry.size))

		if entry.droppable &&
			s.SlowClientMaxQueueAge > 0 &&
			s.timeNow().Sub(entry.enqueuedAt) >= s.SlowClientMaxQueueAge {
			bp.drop(entry)
			return nil
		}

		return entry.run()
	})
	if !ok {
		bp.queuedPackets.Add(-1)
		bp.queuedBytes.Add(int64(-entry.size))

		if s.SlowClientPolicy == ServerSlowClientPolicyDisconnect {
			return bp.disconnect("write queue is full")
		}

		if entry.droppable {
			bp.drop(entry)
			return nil
		}

		return liberrors.ErrServerWriteQueueFull{}
	}

	return nil
}

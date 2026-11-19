package gortsplib

import (
	"container/heap"
	"sync"
	"time"

	"github.com/pion/rtp"

	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/rtpreceiver"
	"github.com/bluenviron/gortsplib/v5/pkg/rtptime"
)

// ClientSyncQueuePolicy is the policy applied by the synchronized packet view
// when its outbound queue is full.
type ClientSyncQueuePolicy int

const (
	// ClientSyncQueuePolicyDropOldest discards the oldest queued packet
	// when the outbound queue is full. Discarded packets are counted in
	// ClientSyncStats.DroppedOverflow.
	ClientSyncQueuePolicyDropOldest ClientSyncQueuePolicy = iota

	// ClientSyncQueuePolicyBlock applies backpressure: inbound packet
	// processing is paused until the consumer frees space in the outbound
	// queue.
	ClientSyncQueuePolicyBlock
)

// ClientOnPacketSyncFunc is the prototype of Client.OnPacketSync.
type ClientOnPacketSyncFunc func(*ClientSyncPacket)

// ClientSyncPacket is an entry of the synchronized packet view.
type ClientSyncPacket struct {
	// media the packet belongs to.
	Media *description.Media
	// format of the packet.
	Format format.Format
	// RTP packet.
	Packet *rtp.Packet

	// PTS is the presentation timestamp of the packet, in clock rate units
	// of the format, synchronized with the other tracks.
	// It is available only when HasPTS is true.
	PTS    int64
	HasPTS bool

	// NTP is the absolute timestamp of the packet, computed from RTCP
	// sender reports. It is available only when the sender provided at
	// least one RTCP sender report for this track (HasNTP is true).
	NTP    time.Time
	HasNTP bool

	// SortTime is the wall-clock time used to sort packets of all tracks
	// against each other. It is the NTP timestamp when available,
	// otherwise it is derived from the PTS, otherwise from the arrival
	// time. Packets are always delivered with non-decreasing SortTime.
	SortTime time.Time
}

// ClientSyncStats are statistics of the synchronized packet view.
type ClientSyncStats struct {
	// packets delivered through OnPacketSync.
	OutputPackets uint64
	// packets discarded because their sort time was earlier than the sort
	// time of the last emitted packet (for instance after a NTP regression
	// or a timestamp reset).
	DroppedLate uint64
	// packets discarded because the outbound queue was full and the policy
	// was ClientSyncQueuePolicyDropOldest.
	DroppedOverflow uint64
}

// avoid an int64 overflow and preserve resolution by splitting division into two parts.
func syncMultiplyAndDivide(v, m, d int64) int64 {
	secs := v / d
	dec := v % d
	return secs*m + dec*m/d
}

type clientSyncHeapItem struct {
	pkt *ClientSyncPacket
	seq uint64
}

// clientSyncHeap is a min-heap of packets, ordered by (SortTime, insertion
// sequence). The insertion sequence makes the sort stable across tracks.
type clientSyncHeap []clientSyncHeapItem

func (h clientSyncHeap) Len() int {
	return len(h)
}

func (h clientSyncHeap) Less(i, j int) bool {
	if !h[i].pkt.SortTime.Equal(h[j].pkt.SortTime) {
		return h[i].pkt.SortTime.Before(h[j].pkt.SortTime)
	}
	return h[i].seq < h[j].seq
}

func (h clientSyncHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *clientSyncHeap) Push(x any) {
	*h = append(*h, x.(clientSyncHeapItem))
}

func (h *clientSyncHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = clientSyncHeapItem{}
	*h = old[:n-1]
	return item
}

// clientSyncView merges inbound RTP packets of all medias into a single
// stream, sorted by wall-clock time.
//
// Packets are held in a reorder buffer until their sort time is older than
// the newest sort time by at least the reorder window, and are then moved
// into a bounded outbound queue, drained by a dedicated goroutine that calls
// the user callback. This guarantees that:
//   - packets are emitted with non-decreasing SortTime;
//   - late packets (sort time earlier than the last emitted one) are
//     discarded and counted;
//   - a stalled track does not block the others: packets still in the
//     reorder buffer are flushed after a window of wall-clock silence;
//   - on close, all queued packets are flushed in order before returning.
type clientSyncView struct {
	c           *Client
	window      time.Duration
	queueSize   int
	policy      ClientSyncQueuePolicy
	timeDecoder *rtptime.GlobalDecoder

	mutex          sync.Mutex
	cond           sync.Cond
	closed         bool
	reorderBuf     clientSyncHeap
	reorderSeq     uint64
	pending        []*ClientSyncPacket
	maxSortTime    time.Time
	hasMaxSortTime bool
	lastEmitted    time.Time
	hasEmitted     bool
	ptsBase        time.Time
	hasPTSBase     bool
	lastPushTime   time.Time

	terminate chan struct{}
	done      chan struct{}
}

func (sv *clientSyncView) initialize() {
	sv.cond.L = &sv.mutex
	heap.Init(&sv.reorderBuf)
	sv.terminate = make(chan struct{})
	sv.done = make(chan struct{})

	go sv.runDispatcher()
	go sv.runFlushTimer()
}

// close flushes all queued packets in sorted order, waits for them to be
// delivered to the callback and releases all resources.
// Packets pushed after close are silently discarded.
func (sv *clientSyncView) close() {
	close(sv.terminate)

	sv.mutex.Lock()
	sv.closed = true
	// flush the reorder buffer in sorted order, bypassing the outbound
	// queue bound, so that every queued packet is delivered.
	for sv.reorderBuf.Len() > 0 {
		item := heap.Pop(&sv.reorderBuf).(clientSyncHeapItem)
		sv.appendPendingLocked(item.pkt)
	}
	sv.cond.Broadcast()
	sv.mutex.Unlock()

	<-sv.done
}

// push adds a packet to the view.
// now is the arrival time of the packet.
func (sv *clientSyncView) push(
	medi *description.Media,
	forma format.Format,
	recv *rtpreceiver.Receiver,
	pkt *rtp.Packet,
	now time.Time,
) {
	pts, hasPTS := sv.timeDecoder.Decode(forma, pkt)
	ntp, hasNTP := recv.PacketNTP(pkt.Timestamp)

	sp := &ClientSyncPacket{
		Media:  medi,
		Format: forma,
		Packet: pkt,
		PTS:    pts,
		HasPTS: hasPTS,
		NTP:    ntp,
		HasNTP: hasNTP,
	}

	sv.mutex.Lock()
	defer sv.mutex.Unlock()

	if sv.closed {
		return
	}

	// compute the sort time.
	// NTP (from RTCP sender reports) has priority; when it is missing, the
	// PTS is mapped to wall-clock by anchoring it to the arrival time of
	// the first packet with a PTS; as a last resort, the arrival time is
	// used.
	switch {
	case hasNTP:
		sp.SortTime = ntp

	case hasPTS:
		ptsDuration := time.Duration(syncMultiplyAndDivide(
			pts, int64(time.Second), int64(forma.ClockRate())))
		if !sv.hasPTSBase {
			sv.ptsBase = now.Add(-ptsDuration)
			sv.hasPTSBase = true
		}
		sp.SortTime = sv.ptsBase.Add(ptsDuration)

	default:
		sp.SortTime = now
	}

	// discard packets that would break output monotonicity.
	// this happens for instance after a NTP regression or a timestamp reset.
	if sv.hasEmitted && sp.SortTime.Before(sv.lastEmitted) {
		sv.c.syncDroppedLate.Add(1)
		return
	}

	heap.Push(&sv.reorderBuf, clientSyncHeapItem{pkt: sp, seq: sv.reorderSeq})
	sv.reorderSeq++

	if !sv.hasMaxSortTime || sp.SortTime.After(sv.maxSortTime) {
		sv.maxSortTime = sp.SortTime
		sv.hasMaxSortTime = true
	}
	sv.lastPushTime = now

	sv.moveExpiredLocked()

	// bound the reorder buffer: force-emit the oldest packets.
	for sv.reorderBuf.Len() > sv.queueSize {
		item := heap.Pop(&sv.reorderBuf).(clientSyncHeapItem)
		sv.emitLocked(item.pkt)
	}
}

// moveExpiredLocked moves packets whose sort time is older than
// (newest sort time - window) from the reorder buffer to the outbound queue.
func (sv *clientSyncView) moveExpiredLocked() {
	if !sv.hasMaxSortTime {
		return
	}

	threshold := sv.maxSortTime.Add(-sv.window)

	for sv.reorderBuf.Len() > 0 && !sv.reorderBuf[0].pkt.SortTime.After(threshold) {
		item := heap.Pop(&sv.reorderBuf).(clientSyncHeapItem)
		sv.emitLocked(item.pkt)
	}
}

// emitLocked moves a packet from the reorder buffer to the outbound queue,
// applying the queue policy.
func (sv *clientSyncView) emitLocked(pkt *ClientSyncPacket) {
	sv.lastEmitted = pkt.SortTime
	sv.hasEmitted = true

	if sv.policy == ClientSyncQueuePolicyBlock {
		for len(sv.pending) >= sv.queueSize && !sv.closed {
			sv.cond.Wait()
		}
		if sv.closed {
			return
		}
	} else if len(sv.pending) >= sv.queueSize {
		// drop the oldest queued packet
		sv.pending[0] = nil
		sv.pending = sv.pending[1:]
		sv.c.syncDroppedOverflow.Add(1)
	}

	sv.pending = append(sv.pending, pkt)
	sv.cond.Signal()
}

// appendPendingLocked appends a packet to the outbound queue without
// applying any bound. Used during close.
func (sv *clientSyncView) appendPendingLocked(pkt *ClientSyncPacket) {
	sv.lastEmitted = pkt.SortTime
	sv.hasEmitted = true
	sv.pending = append(sv.pending, pkt)
}

// flushTick is called periodically and flushes packets that are ready, even
// when no new packet is arriving. It also flushes the whole reorder buffer
// when no packet has been received within a window (stream interrupted),
// bounding the delivery latency of the remaining packets.
func (sv *clientSyncView) flushTick() {
	now := sv.c.timeNow()

	sv.mutex.Lock()
	defer sv.mutex.Unlock()

	if sv.closed {
		return
	}

	sv.moveExpiredLocked()

	if sv.reorderBuf.Len() > 0 && now.Sub(sv.lastPushTime) >= sv.window {
		for sv.reorderBuf.Len() > 0 {
			item := heap.Pop(&sv.reorderBuf).(clientSyncHeapItem)
			sv.emitLocked(item.pkt)
		}
	}
}

func (sv *clientSyncView) runFlushTimer() {
	period := max(sv.window/2, 10*time.Millisecond)
	t := time.NewTicker(period)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			sv.flushTick()

		case <-sv.terminate:
			return
		}
	}
}

func (sv *clientSyncView) runDispatcher() {
	defer close(sv.done)

	for {
		sv.mutex.Lock()
		for len(sv.pending) == 0 && !sv.closed {
			sv.cond.Wait()
		}
		if len(sv.pending) == 0 { // closed and drained
			sv.mutex.Unlock()
			return
		}
		batch := sv.pending
		sv.pending = nil
		sv.cond.Broadcast() // wake producers blocked by ClientSyncQueuePolicyBlock
		sv.mutex.Unlock()

		for _, pkt := range batch {
			sv.c.syncOutputPackets.Add(1)
			sv.c.OnPacketSync(pkt)
		}
	}
}

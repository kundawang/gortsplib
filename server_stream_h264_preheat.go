package gortsplib

import (
	"bytes"
	"sync"
	"time"

	"github.com/pion/rtp"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
)

// bounds of a cached IDR group.
// A group is made of the packets of a single H264 access unit (same RTP timestamp)
// that contains an IDR NALU, plus the in-band SPS/PPS packets that share it.
const (
	// maximum number of RTP packets kept inside a single IDR group.
	// Groups larger than this are discarded, since a new reader could not
	// receive them without stalling the live stream for too long.
	h264PreheatMaxGroupPackets = 4096

	// maximum total size, in bytes, of the packets of a single IDR group.
	// It matches the maximum H264 access unit size allowed by mediacommon.
	h264PreheatMaxGroupSize = h264.MaxAccessUnitSize
)

// h264PreheatCache stores the data needed to make a new reader decode
// the video immediately, without waiting for the next IDR access unit:
//   - the most recent valid SPS and PPS NALUs,
//   - the packets of the most recent complete access unit containing an IDR.
//
// The cache is per-format (therefore per-track) and does not keep any
// goroutine: it is released as soon as the ServerStream that owns it is
// garbage collected, or explicitly reset through reset().
//
// All methods are safe for concurrent use. Writes happen from the routine
// calling ServerStream.WritePacketRTP(), while snapshots are taken from the
// routine handling PLAY when a new reader is activated.
type h264PreheatCache struct {
	mutex sync.Mutex

	// SSRC of the publisher, captured before the stream rewrites it with
	// the local SSRC. A change means the source has been restarted or
	// replaced, so every cached packet becomes unreliable.
	sourceSSRC      uint32
	sourceSSRCValid bool

	// latest valid parameter sets, in raw NALU form (without Annex-B prefix).
	sps []byte
	pps []byte

	// packets of the access unit currently being collected.
	curGroup      []*rtp.Packet
	curGroupSize  int
	curGroupTS    uint32
	curGroupHasTS bool
	curGroupIDR   bool
	curGroupNTP   time.Time

	// NTP timestamp of the most recent processed packet, used to stamp
	// groups finalized by a timestamp change rather than a Marker bit.
	lastSeenNTP time.Time

	// latest complete IDR group, and NTP timestamp of its first packet.
	lastIDRGroup    []*rtp.Packet
	lastIDRGroupNTP time.Time
}

// reset drops every cached packet and parameter set.
// It is used when the source SSRC changes or when an unrecoverable
// packetization error makes the current group unusable.
func (c *h264PreheatCache) reset() {
	c.sps = nil
	c.pps = nil
	c.curGroup = nil
	c.curGroupSize = 0
	c.curGroupHasTS = false
	c.curGroupIDR = false
	c.curGroupNTP = time.Time{}
	c.lastIDRGroup = nil
}

// process ingests a packet coming from the publisher, before the stream
// rewrites its SSRC with the local one. It only inspects the payload and
// stores deep copies, so later reuse of pkt by the caller cannot corrupt
// the cache.
func (c *h264PreheatCache) process(pkt *rtp.Packet, ntp time.Time) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.lastSeenNTP = ntp

	// SSRC switch: the publisher changed. Sequence numbers and parameter
	// sets of the old source cannot be mixed with the new ones.
	if !c.sourceSSRCValid {
		c.sourceSSRC = pkt.SSRC
		c.sourceSSRCValid = true
	} else if pkt.SSRC != c.sourceSSRC {
		c.reset()
		c.sourceSSRC = pkt.SSRC
		c.sourceSSRCValid = true
	}

	if len(pkt.Payload) == 0 {
		c.abortCurrentGroup()
		return
	}

	nalType := h264.NALUType(pkt.Payload[0] & 0x1F)

	switch nalType {
	case h264.NALUTypeSTAPA:
		// STAP-A: one packet contains several single-time NALUs.
		changedParams := c.processSTAPA(pkt.Payload[1:])

		// a parameter set change invalidates any previously cached IDR,
		// which was encoded with the old parameters.
		if changedParams {
			c.lastIDRGroup = nil
		}

		c.openOrContinueGroup(pkt)
		if changedParams {
			c.curGroupIDR = true // parameters are part of the new random-access point
		}
		c.closeGroupIfMarked(pkt, ntp)

	case h264.NALUTypeFUA:
		c.processFUA(pkt, ntp)

	default: // single NAL unit packet
		switch nalType {
		case h264.NALUTypeSPS:
			if !bytes.Equal(c.sps, pkt.Payload) {
				c.sps = append([]byte(nil), pkt.Payload...)
				c.lastIDRGroup = nil
			}

		case h264.NALUTypePPS:
			if !bytes.Equal(c.pps, pkt.Payload) {
				c.pps = append([]byte(nil), pkt.Payload...)
				c.lastIDRGroup = nil
			}
		}

		c.openOrContinueGroup(pkt)
		if nalType == h264.NALUTypeIDR {
			c.curGroupIDR = true
		}
		c.closeGroupIfMarked(pkt, ntp)
	}
}

// processSTAPA extracts SPS/PPS NALUs from a STAP-A payload.
// It returns true when at least one parameter set was replaced.
func (c *h264PreheatCache) processSTAPA(payload []byte) bool {
	changed := false

	for len(payload) >= 2 {
		size := int(payload[0])<<8 | int(payload[1])
		payload = payload[2:]

		if size == 0 || size > len(payload) {
			// malformed STAP-A: stop parsing, the rest cannot be trusted.
			return changed
		}

		nalu := payload[:size]
		payload = payload[size:]

		if len(nalu) == 0 {
			continue
		}

		switch h264.NALUType(nalu[0] & 0x1F) {
		case h264.NALUTypeSPS:
			if !bytes.Equal(c.sps, nalu) {
				c.sps = append([]byte(nil), nalu...)
				changed = true
			}

		case h264.NALUTypePPS:
			if !bytes.Equal(c.pps, nalu) {
				c.pps = append([]byte(nil), nalu...)
				changed = true
			}
		}
	}

	return changed
}

// processFUA tracks a fragmented NALU. A fragment is part of an IDR group
// only when the Start bit is present and the reconstructed NALU type is IDR.
func (c *h264PreheatCache) processFUA(pkt *rtp.Packet, ntp time.Time) {
	if len(pkt.Payload) < 2 {
		c.abortCurrentGroup()
		return
	}

	start := pkt.Payload[1]&0x80 != 0
	end := pkt.Payload[1]&0x40 != 0
	fragType := h264.NALUType(pkt.Payload[1] & 0x1F)

	if start {
		// a new fragmented NALU starts; any packet collected for another
		// access unit is finalized first.
		c.flushGroupOnTimestampChange(pkt)
		c.openOrContinueGroup(pkt)
		if fragType == h264.NALUTypeIDR {
			c.curGroupIDR = true
		}
	} else {
		// continuation fragment without a starting one: a fragment was
		// lost. The group can never be reassembled, discard it.
		if !c.curGroupHasTS || len(c.curGroup) == 0 {
			c.abortCurrentGroup()
			return
		}

		// a sequence number gap means one or more fragments were lost.
		expectedSeq := c.curGroup[len(c.curGroup)-1].SequenceNumber + 1
		if pkt.SequenceNumber != expectedSeq {
			c.abortCurrentGroup()
			return
		}

		// a timestamp change in the middle of a fragmentation is illegal.
		if pkt.Timestamp != c.curGroupTS {
			c.abortCurrentGroup()
			return
		}

		c.curGroup = append(c.curGroup, pkt.Clone())
		c.curGroupSize += len(pkt.Payload)
		if c.groupOutOfBounds() {
			c.abortCurrentGroup()
			return
		}
	}

	if end {
		c.closeGroupIfMarked(pkt, ntp)
	}
}

// openOrContinueGroup starts collecting a new access unit when the timestamp
// changes, and appends a deep copy of the packet to the current one.
func (c *h264PreheatCache) openOrContinueGroup(pkt *rtp.Packet) {
	c.flushGroupOnTimestampChange(pkt)

	if !c.curGroupHasTS {
		c.curGroupHasTS = true
		c.curGroupTS = pkt.Timestamp
		c.curGroupIDR = false
		c.curGroupNTP = c.lastSeenNTP
	}

	c.curGroup = append(c.curGroup, pkt.Clone())
	c.curGroupSize += len(pkt.Payload)

	if c.groupOutOfBounds() {
		c.abortCurrentGroup()
	}
}

// flushGroupOnTimestampChange finalizes the group being collected when the
// incoming packet belongs to a different access unit.
func (c *h264PreheatCache) flushGroupOnTimestampChange(pkt *rtp.Packet) {
	if !c.curGroupHasTS || pkt.Timestamp == c.curGroupTS {
		return
	}

	c.commitCurrentGroup(time.Time{})
}

// closeGroupIfMarked finalizes the group when the packet carries the Marker
// bit, which is the standard end-of-access-unit indicator.
func (c *h264PreheatCache) closeGroupIfMarked(pkt *rtp.Packet, ntp time.Time) {
	if !pkt.Marker || !c.curGroupHasTS {
		return
	}

	if pkt.Timestamp != c.curGroupTS {
		c.abortCurrentGroup()
		return
	}

	c.commitCurrentGroup(ntp)
}

// commitCurrentGroup promotes the current group to the cached IDR group
// when it contains an IDR NALU.
func (c *h264PreheatCache) commitCurrentGroup(ntp time.Time) {
	if c.curGroupIDR && len(c.curGroup) > 0 {
		c.lastIDRGroup = c.curGroup
		if ntp.IsZero() {
			ntp = c.curGroupNTP
		}
		c.lastIDRGroupNTP = ntp
	}
	c.curGroup = nil
	c.curGroupSize = 0
	c.curGroupHasTS = false
	c.curGroupIDR = false
	c.curGroupNTP = time.Time{}
}

func (c *h264PreheatCache) abortCurrentGroup() {
	c.curGroup = nil
	c.curGroupSize = 0
	c.curGroupHasTS = false
	c.curGroupIDR = false
	c.curGroupNTP = time.Time{}
}

func (c *h264PreheatCache) groupOutOfBounds() bool {
	return len(c.curGroup) > h264PreheatMaxGroupPackets ||
		c.curGroupSize > h264PreheatMaxGroupSize
}

// h264PreheatSnapshot is the immutable view of the cache handed to a new
// reader. Every packet is a private deep copy, so the reader can enqueue it
// without synchronizing with further writes to the cache.
type h264PreheatSnapshot struct {
	// latest SPS and PPS in raw NALU form, when both were seen. They are
	// sent before the IDR group, possibly redundantly when the group
	// already carries them in-band, which decoders accept.
	sps []byte
	pps []byte

	// packets of the latest complete IDR access unit, in arrival order.
	idrGroup    []*rtp.Packet
	idrGroupNTP time.Time
}

// snapshot returns a decachable view of the cache.
// It returns nil when no complete IDR group has been seen yet: in that case
// the new reader is simply attached to the live packet flow.
func (c *h264PreheatCache) snapshot() *h264PreheatSnapshot {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.lastIDRGroup) == 0 {
		return nil
	}

	snap := &h264PreheatSnapshot{
		idrGroup:    make([]*rtp.Packet, len(c.lastIDRGroup)),
		idrGroupNTP: c.lastIDRGroupNTP,
	}

	for i, pkt := range c.lastIDRGroup {
		snap.idrGroup[i] = pkt.Clone()
	}

	// SPS/PPS are always re-sent before the IDR group. They are the latest
	// parameter sets observed on the wire and therefore match the cached IDR.
	// When the group already contains them in-band, the decoder simply ignores
	// the redundant declaration, which is harmless and explicitly allowed.
	if c.sps != nil && c.pps != nil {
		snap.sps = append([]byte(nil), c.sps...)
		snap.pps = append([]byte(nil), c.pps...)
	}

	return snap
}

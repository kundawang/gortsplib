package gortsplib

import (
	"time"

	"github.com/pion/rtp"
)

// preheatReader sends the cached H264 keyframe group to a reader that is
// about to be activated, before it can receive any live packet.
//
// Preheat packets keep their original RTP timestamp, sequence number and
// payload: they are a byte-for-byte replay of what was previously sent on the
// wire, so the new reader sees exactly the same data an earlier reader saw.
// The only synthesized packet is the optional STAP-A carrying the cached
// SPS/PPS, emitted when the IDR group does not contain them in-band.
//
// It never touches the other readers: packets are pushed only onto the
// session's own write queue, and stream-level counters are not incremented.
func (st *ServerStream) preheatReader(ss *ServerSession) {
	// UDP-multicast readers share a single multicast flow and cannot be
	// preheated individually.
	if ss.setuppedTransport.Protocol == ProtocolUDPMulticast {
		return
	}

	for medi, rsm := range ss.setuppedMedias {
		sm, ok := st.medias[medi]
		if !ok {
			continue
		}

		for payloadType, rsf := range rsm.formats {
			sf, ok := sm.formats[payloadType]
			if !ok || sf.preheat == nil {
				continue
			}

			snap := sf.preheat.snapshot()
			if snap == nil {
				// no complete IDR group has been seen yet: the reader is
				// attached to the live flow and will decode at the next IDR.
				continue
			}

			sf.sendPreheatSnapshot(ss, rsf, snap)
		}
	}
}

// sendPreheatSnapshot pushes one cached group to a single session format.
// A failure while enqueuing a packet aborts the preheat for this format:
// the session is already going to receive live packets, and an incomplete
// keyframe would be undecodable anyway.
func (ssf *serverStreamFormat) sendPreheatSnapshot(
	ss *ServerSession,
	rsf *serverSessionFormat,
	snap *h264PreheatSnapshot,
) {
	// optional SPS/PPS synthesized as a single STAP-A packet placed before
	// the IDR group, with a sequence number immediately preceding it.
	if snap.sps != nil && snap.pps != nil && len(snap.idrGroup) > 0 {
		first := snap.idrGroup[0]

		stap := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    first.PayloadType,
				SequenceNumber: first.SequenceNumber - 1,
				Timestamp:      first.Timestamp,
				SSRC:           ssf.localSSRC,
			},
			Payload: buildSTAPA(snap.sps, snap.pps),
		}

		if !rsf.enqueuePreheatPacket(ss, stap, snap.idrGroupNTP) {
			return
		}
	}

	for _, pkt := range snap.idrGroup {
		// the cached packet carries the publisher SSRC; restore the local one.
		pkt.SSRC = ssf.localSSRC

		if !rsf.enqueuePreheatPacket(ss, pkt, snap.idrGroupNTP) {
			return
		}
	}
}

// enqueuePreheatPacket marshals, optionally encrypts and pushes a preheat
// packet on the session writer queue, reusing the exact live-write path.
// It returns false when the packet could not be enqueued.
func (ssf *serverSessionFormat) enqueuePreheatPacket(
	ss *ServerSession,
	pkt *rtp.Packet,
	ntp time.Time,
) bool {
	// writePacketRTP takes care of SSRC, marshaling, SRTP encryption,
	// sender-report bookkeeping and queueing, identically to live packets.
	err := ssf.writePacketRTP(pkt, ntp)
	if err != nil {
		ss.onStreamWriteError(err)
		return false
	}

	return true
}

// buildSTAPA builds a RFC 6184 STAP-A packet payload containing the SPS and
// PPS NALUs. The NRI is set to the highest value, since parameter sets are
// reference data.
func buildSTAPA(sps, pps []byte) []byte {
	out := make([]byte, 0, 1+2+len(sps)+2+len(pps))
	out = append(out, 0x60|24) // NRI=3, type=24 (STAP-A)
	out = append(out, byte(len(sps)>>8), byte(len(sps)))
	out = append(out, sps...)
	out = append(out, byte(len(pps)>>8), byte(len(pps)))
	out = append(out, pps...)
	return out
}

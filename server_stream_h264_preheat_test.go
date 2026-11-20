package gortsplib

import (
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
)

// naluBuilder builds RTP/H264 packets for the preheat tests.
type naluBuilder struct {
	ssrc   uint32
	seq    uint16
	marker bool
}

func (b *naluBuilder) single(nalu []byte, ts uint32, marker bool) *rtp.Packet {
	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: b.seq,
			Timestamp:      ts,
			SSRC:           b.ssrc,
			Marker:         marker,
		},
		Payload: append([]byte(nil), nalu...),
	}
	b.seq++
	return pkt
}

// fua splits a NALU into FU-A packets of the given fragment payload size.
func (b *naluBuilder) fua(nalu []byte, ts uint32, fragSize int, marker bool) []*rtp.Packet {
	if len(nalu) == 0 {
		return nil
	}

	nri := nalu[0] & 0x60
	nalType := nalu[0] & 0x1F
	body := nalu[1:]

	var ret []*rtp.Packet

	for offset := 0; offset < len(body); {
		end := offset + fragSize
		if end > len(body) {
			end = len(body)
		}

		indicator := nri | uint8(h264.NALUTypeFUA)
		header := nalType
		if offset == 0 {
			header |= 0x80 // Start
		}
		if end == len(body) {
			header |= 0x40 // End
		}

		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    96,
				SequenceNumber: b.seq,
				Timestamp:      ts,
				SSRC:           b.ssrc,
				Marker:         marker && end == len(body),
			},
			Payload: append([]byte{indicator, header}, body[offset:end]...),
		}
		ret = append(ret, pkt)
		b.seq++
		offset = end
	}

	return ret
}

func (b *naluBuilder) stapA(nalus [][]byte, ts uint32, marker bool) *rtp.Packet {
	payload := []byte{0x60 | uint8(h264.NALUTypeSTAPA)}
	for _, nalu := range nalus {
		payload = append(payload, byte(len(nalu)>>8), byte(len(nalu)))
		payload = append(payload, nalu...)
	}

	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: b.seq,
			Timestamp:      ts,
			SSRC:           b.ssrc,
			Marker:         marker,
		},
		Payload: payload,
	}
	b.seq++
	return pkt
}

func testNalu(typ h264.NALUType, nri byte, size int) []byte {
	nalu := make([]byte, size)
	nalu[0] = (nri << 5) | byte(typ)
	for i := 1; i < size; i++ {
		nalu[i] = byte(i)
	}
	return nalu
}

func TestH264PreheatCacheSingleIDR(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0x11111111}

	sps := testNalu(h264.NALUTypeSPS, 3, 10)
	pps := testNalu(h264.NALUTypePPS, 3, 8)
	idr := testNalu(h264.NALUTypeIDR, 2, 100)
	pframe := testNalu(h264.NALUTypeNonIDR, 2, 50)

	// SPS + PPS in their own access unit, then an IDR access unit without
	// in-band parameter sets.
	c.process(b.single(sps, 1000, true), ntp)
	c.process(b.single(pps, 1000, true), ntp)
	c.process(b.single(idr, 2000, true), ntp)
	c.process(b.single(pframe, 3000, true), ntp)

	snap := c.snapshot()
	require.NotNil(t, snap)
	require.Equal(t, []uint32{2000}, collectTimestamps(snap.idrGroup))
	require.Len(t, snap.idrGroup, 1)
	require.Equal(t, h264.NALUTypeIDR, h264.NALUType(snap.idrGroup[0].Payload[0]&0x1F))

	// SPS/PPS must be synthesized since they are not in-band.
	require.Equal(t, sps, snap.sps)
	require.Equal(t, pps, snap.pps)
}

func TestH264PreheatCacheFragmentedIDR(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0x22222222}

	sps := testNalu(h264.NALUTypeSPS, 3, 10)
	pps := testNalu(h264.NALUTypePPS, 3, 8)
	idr := testNalu(h264.NALUTypeIDR, 2, 100)

	// SPS/PPS aggregated in a STAP-A, IDR split in four FU-A fragments.
	c.process(b.stapA([][]byte{sps, pps}, 1000, true), ntp)
	for _, pkt := range b.fua(idr, 2000, 30, true) {
		c.process(pkt, ntp)
	}

	snap := c.snapshot()
	require.NotNil(t, snap)
	require.Len(t, snap.idrGroup, 4)

	// all cached packets share the IDR timestamp and carry contiguous seq nums
	for i, pkt := range snap.idrGroup {
		require.Equal(t, uint32(2000), pkt.Timestamp)
		require.Equal(t, uint16(1)+uint16(i), pkt.SequenceNumber)
		require.Equal(t, h264.NALUTypeFUA, h264.NALUType(pkt.Payload[0]&0x1F))
	}

	// parameters were in-band (STAP-A of a previous AU): they are still
	// tracked, but must not be re-synthesized since the STAP-A is in group?
	// The STAP-A belongs to a different timestamp/AU, so the IDR group itself
	// does not contain parameters and they must be synthesized.
	require.NotNil(t, snap.sps)
	require.NotNil(t, snap.pps)
}

func TestH264PreheatCacheInBandParameterSets(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0x33333333}

	sps := testNalu(h264.NALUTypeSPS, 3, 10)
	pps := testNalu(h264.NALUTypePPS, 3, 8)
	idr := testNalu(h264.NALUTypeIDR, 2, 100)

	// a single AU: STAP-A(SPS,PPS) and the IDR share the same timestamp.
	c.process(b.stapA([][]byte{sps, pps}, 4000, false), ntp)
	c.process(b.single(idr, 4000, true), ntp)

	snap := c.snapshot()
	require.NotNil(t, snap)
	require.Len(t, snap.idrGroup, 2)

	// SPS/PPS are always re-declared before the cached IDR, even when the
	// group already contains them in-band: the redundant declaration is
	// harmless for H264 decoders and guarantees every preheat is decodable.
	require.Equal(t, sps, snap.sps)
	require.Equal(t, pps, snap.pps)
}

func TestH264PreheatCacheNoIDR(t *testing.T) {
	c := &h264PreheatCache{}
	b := &naluBuilder{ssrc: 0x44444444}

	c.process(b.single(testNalu(h264.NALUTypeSPS, 3, 10), 1000, true), time.Now())
	c.process(b.single(testNalu(h264.NALUTypeNonIDR, 2, 40), 2000, true), time.Now())
	c.process(b.single(testNalu(h264.NALUTypeNonIDR, 2, 40), 3000, true), time.Now())

	require.Nil(t, c.snapshot())
}

func TestH264PreheatCacheLostFragment(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0x55555555}

	idr := testNalu(h264.NALUTypeIDR, 2, 100)
	frags := b.fua(idr, 2000, 30, true)
	require.Len(t, frags, 4)

	// drop the second fragment: the incomplete IDR must not be cached.
	c.process(frags[0], ntp)
	c.process(frags[2], ntp)
	c.process(frags[3], ntp)

	require.Nil(t, c.snapshot())

	// a later, complete IDR is cached normally.
	c.process(b.single(testNalu(h264.NALUTypeIDR, 2, 60), 3000, true), ntp)
	snap := c.snapshot()
	require.NotNil(t, snap)
	require.Len(t, snap.idrGroup, 1)
}

func TestH264PreheatCacheSSRCSwitch(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0x66666666}

	c.process(b.single(testNalu(h264.NALUTypeSPS, 3, 10), 1000, true), ntp)
	c.process(b.single(testNalu(h264.NALUTypeIDR, 2, 60), 2000, true), ntp)
	require.NotNil(t, c.snapshot())

	// publisher restart with a new SSRC: every cached entry is dropped.
	b.ssrc = 0x77777777
	c.process(b.single(testNalu(h264.NALUTypeNonIDR, 2, 40), 3000, true), ntp)
	require.Nil(t, c.snapshot())
	require.Nil(t, c.sps)
	require.Nil(t, c.pps)

	// the new source must be able to populate the cache from scratch.
	c.process(b.single(testNalu(h264.NALUTypeIDR, 2, 60), 4000, true), ntp)
	require.NotNil(t, c.snapshot())
}

func TestH264PreheatCacheParameterSetChange(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0x88888888}

	sps1 := testNalu(h264.NALUTypeSPS, 3, 10)
	pps1 := testNalu(h264.NALUTypePPS, 3, 8)
	idr1 := testNalu(h264.NALUTypeIDR, 2, 60)

	c.process(b.single(sps1, 1000, true), ntp)
	c.process(b.single(pps1, 1000, true), ntp)
	c.process(b.single(idr1, 2000, true), ntp)
	require.NotNil(t, c.snapshot())

	// a new SPS invalidates the old IDR group until a fresh IDR arrives.
	sps2 := testNalu(h264.NALUTypeSPS, 3, 12)
	c.process(b.single(sps2, 3000, true), ntp)
	require.Nil(t, c.snapshot())
	require.Equal(t, sps2, c.sps)

	// new IDR becomes the cache, paired with the new parameters.
	idr2 := testNalu(h264.NALUTypeIDR, 2, 70)
	c.process(b.single(idr2, 4000, true), ntp)
	snap := c.snapshot()
	require.NotNil(t, snap)
	require.Equal(t, sps2, snap.sps)
	require.Equal(t, pps1, snap.pps)
	require.Equal(t, idr2, snap.idrGroup[0].Payload)
}

func TestH264PreheatCacheLatestGroupWins(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0x99999999}

	c.process(b.single(testNalu(h264.NALUTypeIDR, 2, 60), 1000, true), ntp)
	c.process(b.single(testNalu(h264.NALUTypeNonIDR, 2, 30), 2000, true), ntp)
	c.process(b.single(testNalu(h264.NALUTypeIDR, 2, 61), 3000, true), ntp)

	snap := c.snapshot()
	require.NotNil(t, snap)
	require.Len(t, snap.idrGroup, 1)
	require.Equal(t, uint32(3000), snap.idrGroup[0].Timestamp)
}

func TestH264PreheatCacheDeepCopy(t *testing.T) {
	c := &h264PreheatCache{}
	ntp := time.Unix(1000, 0)
	b := &naluBuilder{ssrc: 0xAAAAAAAA}

	pkt := b.single(testNalu(h264.NALUTypeIDR, 2, 60), 1000, true)
	c.process(pkt, ntp)

	// mutate the caller's packet after processing; the cache must be unaffected.
	pkt.Payload[0] = 0xFF
	pkt.Timestamp = 999999

	snap := c.snapshot()
	require.NotNil(t, snap)
	require.NotEqual(t, uint32(999999), snap.idrGroup[0].Timestamp)
	require.NotEqual(t, byte(0xFF), snap.idrGroup[0].Payload[0])

	// mutating a snapshot must not corrupt a later snapshot.
	snap.idrGroup[0].Payload[1] = 0xEE
	snap2 := c.snapshot()
	require.NotEqual(t, byte(0xEE), snap2.idrGroup[0].Payload[1])
}

func collectTimestamps(pkts []*rtp.Packet) []uint32 {
	ret := make([]uint32, len(pkts))
	for i, pkt := range pkts {
		ret[i] = pkt.Timestamp
	}
	return ret
}

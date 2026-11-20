package gortsplib

import (
	"bufio"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/conn"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/bluenviron/gortsplib/v5/pkg/headers"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
)

// preheatTestMedia builds an H264 video media plus an audio media, in order
// to verify that preheating is per-track and never disturbs other medias.
func preheatTestMedia() []*description.Media {
	return []*description.Media{
		{
			Type: description.MediaTypeVideo,
			Formats: []format.Format{&format.H264{
				PayloadTyp:        96,
				SPS:               testH264Media.Formats[0].(*format.H264).SPS,
				PPS:               testH264Media.Formats[0].(*format.H264).PPS,
				PacketizationMode: 1,
			}},
		},
		{
			Type: description.MediaTypeAudio,
			Formats: []format.Format{&format.G711{
				PayloadTyp:   97,
				MULaw:        true,
				SampleRate:   8000,
				ChannelCount: 1,
			}},
		},
	}
}

// freeTCPPort returns a port that is currently free on localhost.
// The port is not reserved, but binding it immediately afterwards makes a
// collision extremely unlikely and keeps these tests independent of the
// fixed 8554 port used by the rest of the suite.
func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

// startPreheatTestServer starts a server with H264 keyframe preheat enabled
// and returns the server, the stream and the RTSP address it listens on.
func startPreheatTestServer(t *testing.T) (*Server, *ServerStream, string) {
	t.Helper()

	var stream *ServerStream
	address := "localhost:" + strconv.Itoa(freeTCPPort(t))

	s := &Server{
		RTSPAddress:         address,
		H264KeyframePreheat: true,
		Handler: &testServerHandler{
			onDescribe: func(_ *ServerHandlerOnDescribeCtx) (*base.Response, *ServerStream, error) {
				return &base.Response{StatusCode: base.StatusOK}, stream, nil
			},
			onSetup: func(_ *ServerHandlerOnSetupCtx) (*base.Response, *ServerStream, error) {
				return &base.Response{StatusCode: base.StatusOK}, stream, nil
			},
			onPlay: func(_ *ServerHandlerOnPlayCtx) (*base.Response, error) {
				return &base.Response{StatusCode: base.StatusOK}, nil
			},
		},
	}

	err := s.Start()
	require.NoError(t, err)

	stream = &ServerStream{
		Server: s,
		Desc:   &description.Session{Medias: preheatTestMedia()},
	}
	err = stream.Initialize()
	require.NoError(t, err)

	return s, stream, address
}

func preheatTCPClient(t *testing.T, address string) (*conn.Conn, net.Conn, string) {
	t.Helper()

	nconn, err := net.Dial("tcp", address)
	require.NoError(t, err)

	c := conn.NewConn(bufio.NewReader(nconn), nconn)

	desc := doDescribe(t, c, false)

	inTH := &headers.Transport{
		Protocol:       headers.TransportProtocolTCP,
		Delivery:       new(headers.TransportDeliveryUnicast),
		Mode:           new(headers.TransportModePlay),
		InterleavedIDs: &[2]int{0, 1},
	}

	res, _ := doSetup(t, c, mediaURL(t, desc.BaseURL, desc.Medias[0]).String(), inTH, "")
	session := readSession(t, res)

	doPlay(t, c, "rtsp://"+address+"/teststream", session)

	return c, nconn, session
}

// readRTPPacket reads interleaved RTP frames on the given (even) channel,
// skipping RTCP frames on the adjacent odd channel.
func readRTPPacket(t *testing.T, c *conn.Conn, channel int) *rtp.Packet {
	t.Helper()

	for {
		f, err := c.ReadInterleavedFrame()
		require.NoError(t, err)

		if f.Channel != channel {
			// RTCP or another track: keep waiting for the wanted channel.
			continue
		}

		var pkt rtp.Packet
		err = pkt.Unmarshal(f.Payload)
		require.NoError(t, err)
		return &pkt
	}
}

// TestServerPlayH264KeyframePreheat simulates a viewer joining a running H264
// live stream halfway through: the first frames it receives must be the
// cached, decodable keyframe group, followed by the live packet flow.
func TestServerPlayH264KeyframePreheat(t *testing.T) {
	s, stream, address := startPreheatTestServer(t)
	defer s.Close()

	// --- publisher side: build a SPS/PPS + fragmented-IDR + P sequence ---
	const ssrc uint32 = 0x12345678
	b := &naluBuilder{ssrc: ssrc}

	sps := testNalu(h264.NALUTypeSPS, 3, 12)
	pps := testNalu(h264.NALUTypePPS, 3, 8)
	idrNALU := testNalu(h264.NALUTypeIDR, 2, 120)
	pNALU := testNalu(h264.NALUTypeNonIDR, 2, 60)

	videoMedia := stream.Desc.Medias[0]
	now := time.Unix(1000, 0)

	// STAP-A(SPS,PPS) at ts=1000, then a 3-fragment FU-A IDR at ts=2000,
	// then a P-frame at ts=3000.
	stap := b.stapA([][]byte{sps, pps}, 1000, true)
	require.NoError(t, stream.WritePacketRTPWithNTP(videoMedia, stap, now))

	idrFrags := b.fua(idrNALU, 2000, 40, true)
	require.Len(t, idrFrags, 3)
	for _, pkt := range idrFrags {
		require.NoError(t, stream.WritePacketRTPWithNTP(videoMedia, pkt, now))
	}

	pPkt := b.single(pNALU, 3000, true)

	// --- viewer joins while the stream is already running ---
	c, nconn, _ := preheatTCPClient(t, address)
	defer nconn.Close()

	// the new reader must first receive a synthesized STAP-A carrying the
	// cached SPS/PPS (the original in-band STAP-A belonged to a previous AU
	// and is not part of the IDR group), followed by the cached IDR group.
	// Everything keeps the original timestamps and contiguous sequence numbers.
	stapGot := readRTPPacket(t, c, 0)
	require.Equal(t, uint32(2000), stapGot.Timestamp)
	require.Equal(t, idrFrags[0].SequenceNumber-1, stapGot.SequenceNumber)
	require.Equal(t, h264.NALUTypeSTAPA, h264.NALUType(stapGot.Payload[0]&0x1F))
	require.NotZero(t, stapGot.SSRC)
	require.NotEqual(t, ssrc, stapGot.SSRC, "SSRC must be rewritten to the local one")

	var got []*rtp.Packet
	got = append(got, stapGot)
	for range idrFrags {
		got = append(got, readRTPPacket(t, c, 0))
	}

	for i := 1; i < len(got); i++ {
		pkt := got[i]
		require.Equal(t, uint32(2000), pkt.Timestamp, "packet %d timestamp", i)
		require.Equal(t, idrFrags[i-1].SequenceNumber, pkt.SequenceNumber, "packet %d seq", i)
		require.NotZero(t, pkt.SSRC)
		require.NotEqual(t, ssrc, pkt.SSRC, "SSRC must be rewritten to the local one")
		require.Equal(t, h264.NALUTypeFUA, h264.NALUType(pkt.Payload[0]&0x1F))
	}

	// the marker must close the group on the last cached fragment.
	require.True(t, got[len(got)-1].Marker)

	// the cached group must be decodable by a standard RTP/H264 decoder and
	// reconstruct the exact IDR NALU that was published.
	dec := &rtph264.Decoder{}
	require.NoError(t, dec.Init())

	var au [][]byte
	for _, pkt := range got {
		var err error
		au, err = dec.Decode(pkt)
		if err != nil && err != rtph264.ErrMorePacketsNeeded {
			t.Fatalf("decoder error: %v", err)
		}
	}
	require.NotNil(t, au)
	require.Contains(t, au, idrNALU)

	// --- then the live packet flow must continue unchanged ---
	// the P-frame is published only after the join, so it must arrive after
	// the whole cached group, with its original timestamp and sequence number.
	require.NoError(t, stream.WritePacketRTPWithNTP(videoMedia, pPkt, now))
	live := readRTPPacket(t, c, 0)
	require.Equal(t, uint32(3000), live.Timestamp)
	require.Equal(t, pPkt.SequenceNumber, live.SequenceNumber)
	require.Equal(t, h264.NALUTypeNonIDR, h264.NALUType(live.Payload[0]&0x1F))
	require.Equal(t, pPkt.Payload, live.Payload)
}

// TestServerPlayH264KeyframePreheatNoIDR verifies that, when no IDR has been
// published yet, a new reader simply receives the live flow without any
// spurious preheat packet.
func TestServerPlayH264KeyframePreheatNoIDR(t *testing.T) {
	s, stream, address := startPreheatTestServer(t)
	defer s.Close()

	videoMedia := stream.Desc.Medias[0]
	b := &naluBuilder{ssrc: 0x11111111}

	// only P-frames, no keyframe.
	require.NoError(t, stream.WritePacketRTP(videoMedia,
		b.single(testNalu(h264.NALUTypeNonIDR, 2, 60), 4000, true)))

	c, nconn, _ := preheatTCPClient(t, address)
	defer nconn.Close()

	// a live P-frame written after the join must arrive unchanged, proving
	// that nothing was replayed from the empty cache.
	pPkt := b.single(testNalu(h264.NALUTypeNonIDR, 2, 60), 5000, true)
	require.NoError(t, stream.WritePacketRTP(videoMedia, pPkt))

	got := readRTPPacket(t, c, 0)
	require.Equal(t, pPkt.SequenceNumber, got.SequenceNumber)
	require.Equal(t, uint32(5000), got.Timestamp)
}

// TestServerPlayH264KeyframePreheatExistingReader verifies that a reader
// already connected when a new viewer joins keeps its own timeline and
// packet data, and never receives the replayed cached group.
func TestServerPlayH264KeyframePreheatExistingReader(t *testing.T) {
	s, stream, address := startPreheatTestServer(t)
	defer s.Close()

	videoMedia := stream.Desc.Medias[0]

	// first reader connects before any packet.
	c1, nconn1, _ := preheatTCPClient(t, address)
	defer nconn1.Close()

	b := &naluBuilder{ssrc: 0x22222222}
	spsNALU2 := testNalu(h264.NALUTypeSPS, 3, 10)
	ppsNALU2 := testNalu(h264.NALUTypePPS, 3, 8)
	spsPkt := b.single(spsNALU2, 6900, true)
	ppsPkt := b.single(ppsNALU2, 6900, true)
	require.NoError(t, stream.WritePacketRTP(videoMedia, spsPkt))
	require.NoError(t, stream.WritePacketRTP(videoMedia, ppsPkt))
	idr := b.single(testNalu(h264.NALUTypeIDR, 2, 60), 7000, true)
	require.NoError(t, stream.WritePacketRTP(videoMedia, idr))

	// reader 1 gets the three live packets exactly once and in order.
	got1sps := readRTPPacket(t, c1, 0)
	require.Equal(t, h264.NALUTypeSPS, h264.NALUType(got1sps.Payload[0]&0x1F))
	got1pps := readRTPPacket(t, c1, 0)
	require.Equal(t, h264.NALUTypePPS, h264.NALUType(got1pps.Payload[0]&0x1F))
	got1 := readRTPPacket(t, c1, 0)
	require.Equal(t, idr.SequenceNumber, got1.SequenceNumber)

	// second reader joins and must be preheated: synthesized STAP-A first,
	// then the cached IDR with its original timestamp and sequence number.
	c2, nconn2, _ := preheatTCPClient(t, address)
	defer nconn2.Close()
	got2stap := readRTPPacket(t, c2, 0)
	require.Equal(t, h264.NALUTypeSTAPA, h264.NALUType(got2stap.Payload[0]&0x1F))
	require.Equal(t, idr.SequenceNumber-1, got2stap.SequenceNumber)
	got2 := readRTPPacket(t, c2, 0)
	require.Equal(t, idr.SequenceNumber, got2.SequenceNumber)
	require.Equal(t, uint32(7000), got2.Timestamp)

	// a subsequent live packet reaches both readers, with identical data,
	// and reader 1 never sees a duplicated/replayed packet.
	pPkt := b.single(testNalu(h264.NALUTypeNonIDR, 2, 40), 8000, true)
	require.NoError(t, stream.WritePacketRTP(videoMedia, pPkt))

	live1 := readRTPPacket(t, c1, 0)
	live2 := readRTPPacket(t, c2, 0)
	require.Equal(t, pPkt.SequenceNumber, live1.SequenceNumber)
	require.Equal(t, pPkt.SequenceNumber, live2.SequenceNumber)
	require.Equal(t, pPkt.Payload, live1.Payload)
	require.Equal(t, pPkt.Payload, live2.Payload)
}

// TestServerPlayH264KeyframePreheatSSRCSwitch verifies that a publisher SSRC
// switch clears the cache, so a joining reader does not receive a mix of two
// sources.
func TestServerPlayH264KeyframePreheatSSRCSwitch(t *testing.T) {
	s, stream, address := startPreheatTestServer(t)
	defer s.Close()

	videoMedia := stream.Desc.Medias[0]

	b1 := &naluBuilder{ssrc: 0x33333333}
	require.NoError(t, stream.WritePacketRTP(videoMedia,
		b1.single(testNalu(h264.NALUTypeIDR, 2, 60), 9000, true)))

	// publisher restarts with a new SSRC.
	b2 := &naluBuilder{ssrc: 0x44444444}
	require.NoError(t, stream.WritePacketRTP(videoMedia,
		b2.single(testNalu(h264.NALUTypeNonIDR, 2, 40), 9500, true)))

	c, nconn, _ := preheatTCPClient(t, address)
	defer nconn.Close()

	// no stale IDR is replayed: the first packet is a live P-frame written
	// after the join.
	pPkt := b2.single(testNalu(h264.NALUTypeNonIDR, 2, 40), 9600, true)
	require.NoError(t, stream.WritePacketRTP(videoMedia, pPkt))

	got := readRTPPacket(t, c, 0)
	require.Equal(t, pPkt.SequenceNumber, got.SequenceNumber)
	require.Equal(t, uint32(9600), got.Timestamp)
}

// TestServerPlayH264KeyframePreheatConcurrent exercises concurrent packet
// writes while readers join, under the race detector.
func TestServerPlayH264KeyframePreheatConcurrent(t *testing.T) {
	s, stream, address := startPreheatTestServer(t)
	defer s.Close()

	videoMedia := stream.Desc.Medias[0]

	var wg sync.WaitGroup
	terminate := make(chan struct{})

	// two publishers continuously writing IDR/P frames.
	for p := 0; p < 2; p++ {
		wg.Add(1)
		go func(ssrc uint32) {
			defer wg.Done()
			b := &naluBuilder{ssrc: ssrc}
			ts := uint32(0)
			for {
				select {
				case <-terminate:
					return
				default:
				}

				// distinct SSRCs would reset each other's cache; use the same
				// SSRC and varying content instead.
				require.NoError(t, stream.WritePacketRTP(videoMedia,
					b.single(testNalu(h264.NALUTypeIDR, 2, 40), ts, true)))
				ts += 3600
			}
		}(0x55555555)
	}

	// readers continuously joining and leaving.
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				c, nconn, _ := preheatTCPClient(t, address)
				_ = readRTPPacket(t, c, 0)
				nconn.Close()
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(terminate)
	wg.Wait()
}

// TestServerPlayH264KeyframePreheatCloseRelease verifies that closing the
// stream releases the preheat caches, so no cached packet is retained.
func TestServerPlayH264KeyframePreheatCloseRelease(t *testing.T) {
	s, stream, _ := startPreheatTestServer(t)
	defer s.Close()

	videoMedia := stream.Desc.Medias[0]
	sf := stream.medias[videoMedia].formats[96]
	require.NotNil(t, sf.preheat)

	b := &naluBuilder{ssrc: 0x77777777}
	require.NoError(t, stream.WritePacketRTP(videoMedia,
		b.single(testNalu(h264.NALUTypeIDR, 2, 60), 9000, true)))
	require.NotNil(t, sf.preheat.snapshot())

	stream.Close()
	require.Nil(t, sf.preheat, "cache must be released on stream Close")
}

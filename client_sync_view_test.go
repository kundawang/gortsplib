package gortsplib

import (
	"bufio"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/conn"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/headers"
	"github.com/bluenviron/gortsplib/v5/pkg/ntp"
	"github.com/bluenviron/gortsplib/v5/pkg/rtpreceiver"
	"github.com/bluenviron/gortsplib/v5/pkg/rtptime"
)

var testSyncVideoMedia = &description.Media{
	Type: description.MediaTypeVideo,
	Formats: []format.Format{&format.H264{
		PayloadTyp: 96,
		SPS: []byte{
			0x67, 0x42, 0xc0, 0x28, 0xd9, 0x00, 0x78, 0x02,
			0x27, 0xe5, 0x84, 0x00, 0x00, 0x03, 0x00, 0x04,
			0x00, 0x00, 0x03, 0x00, 0xf0, 0x3c, 0x60, 0xc9,
			0x20,
		},
		PPS: []byte{
			0x44, 0x01, 0xc0, 0x25, 0x2f, 0x05, 0x32, 0x40,
		},
		PacketizationMode: 1,
	}},
}

var testSyncAudioMedia = &description.Media{
	Type:    description.MediaTypeAudio,
	Formats: []format.Format{&format.G711{PayloadTyp: 8, SampleRate: 8000, ChannelCount: 1}},
}

type syncPacketCollector struct {
	mutex sync.Mutex
	pkts  []*ClientSyncPacket
}

func (sc *syncPacketCollector) add(pkt *ClientSyncPacket) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	sc.pkts = append(sc.pkts, pkt)
}

func (sc *syncPacketCollector) all() []*ClientSyncPacket {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	ret := make([]*ClientSyncPacket, len(sc.pkts))
	copy(ret, sc.pkts)
	return ret
}

func (sc *syncPacketCollector) count() int {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()
	return len(sc.pkts)
}

func newTestSyncView(
	window time.Duration,
	queueSize int,
	policy ClientSyncQueuePolicy,
	cb ClientOnPacketSyncFunc,
) (*Client, *clientSyncView) {
	c := &Client{
		OnPacketSync: cb,
	}
	c.timeNow = time.Now

	td := &rtptime.GlobalDecoder{}
	td.Initialize()

	sv := &clientSyncView{
		c:           c,
		window:      window,
		queueSize:   queueSize,
		policy:      policy,
		timeDecoder: td,
	}
	sv.initialize()

	return c, sv
}

func testSyncReceiver(t *testing.T, clockRate int) *rtpreceiver.Receiver {
	r := &rtpreceiver.Receiver{
		ClockRate: clockRate,
		Period:    time.Hour,
	}
	require.NoError(t, r.Initialize())
	t.Cleanup(r.Close)
	return r
}

func testSyncRTPPacket(seq uint16, ts uint32) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      ts,
			SSRC:           753621,
		},
		Payload: []byte{0x65, 1, 2, 3}, // IDR NALU, so that PTSEqualsDTS is true
	}
}

// requireMonotonicSortTime checks that packets are delivered with
// non-decreasing sort times.
func requireMonotonicSortTime(t *testing.T, pkts []*ClientSyncPacket) {
	t.Helper()
	for i := 1; i < len(pkts); i++ {
		require.False(t, pkts[i].SortTime.Before(pkts[i-1].SortTime),
			"packet %d sort time %v is before previous %v", i, pkts[i].SortTime, pkts[i-1].SortTime)
	}
}

func TestClientSyncViewPTSFallback(t *testing.T) {
	var col syncPacketCollector
	c, sv := newTestSyncView(50*time.Millisecond, 16, ClientSyncQueuePolicyDropOldest, col.add)

	vrecv := testSyncReceiver(t, 90000)
	arecv := testSyncReceiver(t, 8000)
	vf := testSyncVideoMedia.Formats[0]
	af := testSyncAudioMedia.Formats[0]

	// no RTCP sender reports: packets must be sorted with the PTS,
	// normalized across tracks with different clock rates.
	now := time.Now()
	push := func(medi *description.Media, forma format.Format, recv *rtpreceiver.Receiver, seq uint16, ts uint32) {
		sv.push(medi, forma, recv, testSyncRTPPacket(seq, ts), now)
		now = now.Add(time.Millisecond)
	}

	push(testSyncVideoMedia, vf, vrecv, 1, 90000)  // 1s
	push(testSyncAudioMedia, af, arecv, 1, 8000)   // 1s
	push(testSyncVideoMedia, vf, vrecv, 3, 270000) // 3s
	push(testSyncAudioMedia, af, arecv, 2, 16000)  // 2s
	push(testSyncVideoMedia, vf, vrecv, 2, 180000) // 2s
	push(testSyncAudioMedia, af, arecv, 3, 24000)  // 3s

	sv.close()

	pkts := col.all()
	require.Len(t, pkts, 6)
	requireMonotonicSortTime(t, pkts)

	for _, pkt := range pkts {
		require.True(t, pkt.HasPTS)
		require.False(t, pkt.HasNTP)
	}

	// per-track order must be preserved
	var videoPTS, audioPTS []int64
	for _, pkt := range pkts {
		switch pkt.Media {
		case testSyncVideoMedia:
			videoPTS = append(videoPTS, pkt.PTS)
		case testSyncAudioMedia:
			audioPTS = append(audioPTS, pkt.PTS)
		}
	}
	require.Equal(t, []int64{0, 90000, 180000}, videoPTS)
	require.Greater(t, videoPTS[1], videoPTS[0])
	require.Greater(t, videoPTS[2], videoPTS[1])
	require.Len(t, audioPTS, 3)
	require.Greater(t, audioPTS[1], audioPTS[0])
	require.Greater(t, audioPTS[2], audioPTS[1])

	stats := c.SyncStats()
	require.Equal(t, uint64(6), stats.OutputPackets)
	require.Equal(t, uint64(0), stats.DroppedLate)
	require.Equal(t, uint64(0), stats.DroppedOverflow)
}

func TestClientSyncViewNTP(t *testing.T) {
	var col syncPacketCollector
	c, sv := newTestSyncView(50*time.Millisecond, 16, ClientSyncQueuePolicyDropOldest, col.add)

	vrecv := testSyncReceiver(t, 90000)
	arecv := testSyncReceiver(t, 8000)
	vf := testSyncVideoMedia.Formats[0]
	af := testSyncAudioMedia.Formats[0]

	t0 := time.Date(2020, 5, 10, 12, 0, 0, 0, time.UTC)

	vrecv.ProcessSenderReport(&rtcp.SenderReport{
		SSRC:    753621,
		NTPTime: ntp.Encode(t0),
		RTPTime: 0,
	}, time.Now())
	arecv.ProcessSenderReport(&rtcp.SenderReport{
		SSRC:    984561,
		NTPTime: ntp.Encode(t0),
		RTPTime: 0,
	}, time.Now())

	now := time.Now()
	push := func(medi *description.Media, forma format.Format, recv *rtpreceiver.Receiver, seq uint16, ts uint32) {
		sv.push(medi, forma, recv, testSyncRTPPacket(seq, ts), now)
		now = now.Add(time.Millisecond)
	}

	// scrambled arrival order; NTP timestamps must determine the output order.
	push(testSyncAudioMedia, af, arecv, 1, 4000)  // t0 + 0.5s
	push(testSyncVideoMedia, vf, vrecv, 1, 90000) // t0 + 1s
	push(testSyncVideoMedia, vf, vrecv, 2, 45000) // t0 + 0.5s
	push(testSyncAudioMedia, af, arecv, 2, 8000)  // t0 + 1s

	sv.close()

	pkts := col.all()
	require.Len(t, pkts, 4)
	requireMonotonicSortTime(t, pkts)

	// ties are broken by insertion order (stable sort)
	require.Equal(t, testSyncAudioMedia, pkts[0].Media)
	require.Equal(t, uint32(4000), pkts[0].Packet.Timestamp)
	require.Equal(t, testSyncVideoMedia, pkts[1].Media)
	require.Equal(t, uint32(45000), pkts[1].Packet.Timestamp)
	require.Equal(t, testSyncVideoMedia, pkts[2].Media)
	require.Equal(t, uint32(90000), pkts[2].Packet.Timestamp)
	require.Equal(t, testSyncAudioMedia, pkts[3].Media)
	require.Equal(t, uint32(8000), pkts[3].Packet.Timestamp)

	for i, pkt := range pkts {
		require.True(t, pkt.HasNTP)
		require.True(t, pkt.SortTime.Equal(t0.Add(time.Duration(i/2+1)*500*time.Millisecond)),
			"packet %d: expected %v, got %v", i, t0.Add(time.Duration(i/2+1)*500*time.Millisecond), pkt.SortTime)
	}

	stats := c.SyncStats()
	require.Equal(t, uint64(4), stats.OutputPackets)
	require.Equal(t, uint64(0), stats.DroppedLate)
}

func TestClientSyncViewNTPRegression(t *testing.T) {
	var col syncPacketCollector
	c, sv := newTestSyncView(50*time.Millisecond, 16, ClientSyncQueuePolicyDropOldest, col.add)

	vrecv := testSyncReceiver(t, 90000)
	vf := testSyncVideoMedia.Formats[0]

	t0 := time.Date(2020, 5, 10, 12, 0, 0, 0, time.UTC)

	vrecv.ProcessSenderReport(&rtcp.SenderReport{
		SSRC:    753621,
		NTPTime: ntp.Encode(t0),
		RTPTime: 0,
	}, time.Now())

	now := time.Now()
	sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(1, 90000), now)
	sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(2, 135000), now)

	// NTP moves backwards: a new sender report maps timestamps to an
	// earlier absolute time.
	vrecv.ProcessSenderReport(&rtcp.SenderReport{
		SSRC:    753621,
		NTPTime: ntp.Encode(t0.Add(-5 * time.Second)),
		RTPTime: 135000,
	}, time.Now())

	// this packet gets a sort time earlier than the last emitted one:
	// it must be discarded to preserve output monotonicity.
	sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(3, 180000), now)

	sv.close()

	pkts := col.all()
	require.Len(t, pkts, 2)
	requireMonotonicSortTime(t, pkts)
	require.Equal(t, uint32(90000), pkts[0].Packet.Timestamp)
	require.Equal(t, uint32(135000), pkts[1].Packet.Timestamp)

	stats := c.SyncStats()
	require.Equal(t, uint64(2), stats.OutputPackets)
	require.Equal(t, uint64(1), stats.DroppedLate)
}

func TestClientSyncViewTimestampWrap(t *testing.T) {
	t.Run("pts", func(t *testing.T) {
		var col syncPacketCollector
		c, sv := newTestSyncView(50*time.Millisecond, 16, ClientSyncQueuePolicyDropOldest, col.add)

		vrecv := testSyncReceiver(t, 90000)
		vf := testSyncVideoMedia.Formats[0]

		now := time.Now()
		sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(1, 0xFFFFFF00), now)
		sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(2, 0x00000100), now) // wrapped

		sv.close()

		pkts := col.all()
		require.Len(t, pkts, 2)
		requireMonotonicSortTime(t, pkts)
		require.Equal(t, int64(0), pkts[0].PTS)
		require.Equal(t, int64(0x200), pkts[1].PTS)

		stats := c.SyncStats()
		require.Equal(t, uint64(0), stats.DroppedLate)
	})

	t.Run("ntp", func(t *testing.T) {
		var col syncPacketCollector
		_, sv := newTestSyncView(50*time.Millisecond, 16, ClientSyncQueuePolicyDropOldest, col.add)

		vrecv := testSyncReceiver(t, 90000)
		vf := testSyncVideoMedia.Formats[0]

		t0 := time.Date(2020, 5, 10, 12, 0, 0, 0, time.UTC)

		vrecv.ProcessSenderReport(&rtcp.SenderReport{
			SSRC:    753621,
			NTPTime: ntp.Encode(t0),
			RTPTime: 0xFFFFFF00,
		}, time.Now())

		now := time.Now()
		sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(1, 0xFFFFFF00), now)
		sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(2, 0x00000100), now) // wrapped

		sv.close()

		pkts := col.all()
		require.Len(t, pkts, 2)
		requireMonotonicSortTime(t, pkts)
		require.True(t, pkts[0].SortTime.Equal(t0))
		require.WithinDuration(t, t0.Add(512*time.Second/90000), pkts[1].SortTime, time.Millisecond)
	})
}

func TestClientSyncViewQueueDropOldest(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once

	var col syncPacketCollector
	c, sv := newTestSyncView(time.Hour, 4, ClientSyncQueuePolicyDropOldest, func(pkt *ClientSyncPacket) {
		// block the first delivery to simulate a slow consumer
		once.Do(func() { <-release })
		col.add(pkt)
	})

	vrecv := testSyncReceiver(t, 90000)
	vf := testSyncVideoMedia.Formats[0]

	now := time.Now()
	for i := range 10 {
		sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(uint16(i+1), uint32(i)*9000), now)
	}

	// the consumer is still blocked: nothing has been delivered yet
	require.Equal(t, 0, col.count())

	close(release)
	sv.close()

	pkts := col.all()
	require.Len(t, pkts, 8)
	requireMonotonicSortTime(t, pkts)

	// the two oldest queued packets have been discarded
	expectedPTS := make([]int64, 8)
	for i := range 8 {
		expectedPTS[i] = int64(i+2) * 9000
	}
	actualPTS := make([]int64, 8)
	for i, pkt := range pkts {
		actualPTS[i] = pkt.PTS
	}
	require.Equal(t, expectedPTS, actualPTS)

	stats := c.SyncStats()
	require.Equal(t, uint64(8), stats.OutputPackets)
	require.Equal(t, uint64(2), stats.DroppedOverflow)
}

func TestClientSyncViewBackpressure(t *testing.T) {
	consume := make(chan struct{})

	var col syncPacketCollector
	c, sv := newTestSyncView(time.Hour, 2, ClientSyncQueuePolicyBlock, func(pkt *ClientSyncPacket) {
		<-consume // each packet needs a token to be consumed
		col.add(pkt)
	})

	vrecv := testSyncReceiver(t, 90000)
	vf := testSyncVideoMedia.Formats[0]

	pusherDone := make(chan struct{})
	go func() {
		defer close(pusherDone)
		now := time.Now()
		for i := range 8 {
			sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(uint16(i+1), uint32(i)*9000), now)
		}
	}()

	// the outbound queue fills up and the pusher blocks (backpressure)
	time.Sleep(200 * time.Millisecond)
	select {
	case <-pusherDone:
		t.Fatal("pusher should be blocked by a full outbound queue")
	default:
	}
	require.Equal(t, 0, col.count())

	// drain the queue
	go func() {
		for range 8 {
			consume <- struct{}{}
		}
	}()

	<-pusherDone
	sv.close()

	pkts := col.all()
	require.Len(t, pkts, 8)
	requireMonotonicSortTime(t, pkts)

	stats := c.SyncStats()
	require.Equal(t, uint64(8), stats.OutputPackets)
	require.Equal(t, uint64(0), stats.DroppedOverflow)
}

func TestClientSyncViewCloseFlush(t *testing.T) {
	var col syncPacketCollector
	c, sv := newTestSyncView(time.Hour, 16, ClientSyncQueuePolicyDropOldest, col.add)

	vrecv := testSyncReceiver(t, 90000)
	vf := testSyncVideoMedia.Formats[0]

	now := time.Now()
	sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(2, 180000), now) // PTS 0
	sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(1, 90000), now)  // PTS -90000
	sv.push(testSyncVideoMedia, vf, vrecv, testSyncRTPPacket(3, 270000), now) // PTS 90000

	// close must flush all queued packets in sorted order.
	sv.close()

	pkts := col.all()
	require.Len(t, pkts, 3)
	requireMonotonicSortTime(t, pkts)
	require.Equal(t, []int64{-90000, 0, 90000}, []int64{pkts[0].PTS, pkts[1].PTS, pkts[2].PTS})

	stats := c.SyncStats()
	require.Equal(t, uint64(3), stats.OutputPackets)
}

func TestClientPlaySyncView(t *testing.T) {
	l, err := net.Listen("tcp", "localhost:8554")
	require.NoError(t, err)
	defer l.Close()

	serverDone := make(chan struct{})
	defer func() { <-serverDone }()

	// absolute time shared by both tracks through RTCP sender reports.
	// it is deliberately in the future, so that the first packets of each
	// track (sent before the sender reports, sorted with the PTS fallback)
	// always sort before all NTP-anchored packets.
	t0 := time.Now().Add(2 * time.Second).UTC().Truncate(time.Second)

	audioMedia := &description.Media{
		Type:    description.MediaTypeAudio,
		Formats: []format.Format{&format.G711{PayloadTyp: 8, SampleRate: 8000, ChannelCount: 1}},
	}

	go func() {
		defer close(serverDone)

		nconn, err2 := l.Accept()
		require.NoError(t, err2)
		defer nconn.Close()
		conn := conn.NewConn(bufio.NewReader(nconn), nconn)

		req, err2 := conn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Options, req.Method)

		err2 = conn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Public": base.HeaderValue{strings.Join([]string{
					string(base.Describe),
					string(base.Setup),
					string(base.Play),
					string(base.Pause),
				}, ", ")},
			},
		})
		require.NoError(t, err2)

		req, err2 = conn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Describe, req.Method)

		medias := []*description.Media{testSyncVideoMedia, audioMedia}

		err2 = conn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
			Header: base.Header{
				"Content-Type": base.HeaderValue{"application/sdp"},
				"Content-Base": base.HeaderValue{"rtsp://localhost:8554/teststream/"},
			},
			Body: mediasToSDP(medias),
		})
		require.NoError(t, err2)

		l1, err2 := net.ListenPacket("udp", "localhost:27556")
		require.NoError(t, err2)
		defer l1.Close()

		l2, err2 := net.ListenPacket("udp", "localhost:27557")
		require.NoError(t, err2)
		defer l2.Close()

		l3, err2 := net.ListenPacket("udp", "localhost:27558")
		require.NoError(t, err2)
		defer l3.Close()

		l4, err2 := net.ListenPacket("udp", "localhost:27559")
		require.NoError(t, err2)
		defer l4.Close()

		var clientPorts [2][2]int

		for i := range 2 {
			req, err2 = conn.ReadRequest()
			require.NoError(t, err2)
			require.Equal(t, base.Setup, req.Method)

			var inTH headers.Transport
			err2 = inTH.Unmarshal(req.Header["Transport"])
			require.NoError(t, err2)

			clientPorts[i] = *inTH.ClientPorts

			err2 = conn.WriteResponse(&base.Response{
				StatusCode: base.StatusOK,
				Header: base.Header{
					"Transport": headers.Transport{
						Protocol:    headers.TransportProtocolUDP,
						Delivery:    new(headers.TransportDeliveryUnicast),
						ServerPorts: &[2]int{27556 + i*2, 27557 + i*2},
						ClientPorts: inTH.ClientPorts,
					}.Marshal(),
				},
			})
			require.NoError(t, err2)
		}

		req, err2 = conn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Play, req.Method)

		err2 = conn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		})
		require.NoError(t, err2)

		videoAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: clientPorts[0][0]}
		videoRTCPAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: clientPorts[0][1]}
		audioAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: clientPorts[1][0]}
		audioRTCPAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: clientPorts[1][1]}

		writeVideo := func(seq uint16, ts uint32) {
			_, err2 = l1.WriteTo(mustMarshalPacketRTP(&rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					Marker:         true,
					PayloadType:    96,
					SequenceNumber: seq,
					Timestamp:      ts,
					SSRC:           753621,
				},
				Payload: []byte{0x65, 1, 2, 3},
			}), videoAddr)
			require.NoError(t, err2)
		}

		writeAudio := func(seq uint16, ts uint32) {
			_, err2 = l3.WriteTo(mustMarshalPacketRTP(&rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    8,
					SequenceNumber: seq,
					Timestamp:      ts,
					SSRC:           984561,
				},
				Payload: []byte{1, 2, 3, 4},
			}), audioAddr)
			require.NoError(t, err2)
		}

		// anchor packets: they make the client learn the SSRC of each track
		writeVideo(1000, 45000) // t0 + 0.5s
		writeAudio(5000, 4400)  // t0 + 0.55s

		time.Sleep(100 * time.Millisecond)

		_, err2 = l2.WriteTo(mustMarshalPacketRTCP(&rtcp.SenderReport{
			SSRC:        753621,
			NTPTime:     ntp.Encode(t0),
			RTPTime:     0,
			PacketCount: 1,
			OctetCount:  4,
		}), videoRTCPAddr)
		require.NoError(t, err2)

		_, err2 = l4.WriteTo(mustMarshalPacketRTCP(&rtcp.SenderReport{
			SSRC:        984561,
			NTPTime:     ntp.Encode(t0),
			RTPTime:     0,
			PacketCount: 1,
			OctetCount:  4,
		}), audioRTCPAddr)
		require.NoError(t, err2)

		time.Sleep(100 * time.Millisecond)

		// inject reordering and a duplicate packet
		writeVideo(1002, 135000) // t0 + 1.5s
		writeAudio(5001, 8400)   // t0 + 1.05s
		writeAudio(5001, 8400)   // duplicate
		writeVideo(1001, 90000)  // t0 + 1s
		writeAudio(5002, 12400)  // t0 + 1.55s

		time.Sleep(200 * time.Millisecond)

		// the audio track stops (stream interruption); video must keep flowing
		writeVideo(1003, 180000) // t0 + 2s
		writeVideo(1004, 225000) // t0 + 2.5s

		req, err2 = conn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Pause, req.Method)

		err2 = conn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		})
		require.NoError(t, err2)

		req, err2 = conn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Play, req.Method)

		err2 = conn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		})
		require.NoError(t, err2)

		// after a pause/play cycle, delivery must resume deterministically
		writeVideo(1005, 270000) // t0 + 3s

		req, err2 = conn.ReadRequest()
		require.NoError(t, err2)
		require.Equal(t, base.Teardown, req.Method)

		err2 = conn.WriteResponse(&base.Response{
			StatusCode: base.StatusOK,
		})
		require.NoError(t, err2)
	}()

	var col syncPacketCollector

	c := Client{
		SyncReorderWindow: 600 * time.Millisecond,
		SyncQueueSize:     64,
		OnPacketSync:      col.add,
	}

	var callbackCount atomic.Int64

	err = readAll(&c, "rtsp://localhost:8554/teststream",
		func(_ *description.Media, _ format.Format, _ *rtp.Packet) {
			callbackCount.Add(1)
		})
	require.NoError(t, err)
	defer c.Close()

	// wait for all packets, including the ones flushed after the audio
	// track stops (stall flush) — this must happen before Close().
	waitFor := func(n int) {
		deadline := time.Now().Add(5 * time.Second)
		for col.count() < n && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if col.count() != n {
			for i, pkt := range col.all() {
				t.Logf("have pkt %d: %v seq=%d sort=%v", i, pkt.Media.Type, pkt.Packet.SequenceNumber, pkt.SortTime)
			}
			t.Logf("stats: %+v", c.SyncStats())
		}
		require.Equal(t, n, col.count())
	}
	waitFor(8)

	// pause and play again: the view is recreated and keeps working
	_, err = c.Pause()
	require.NoError(t, err)

	_, err = c.Play(nil)
	require.NoError(t, err)

	waitFor(9)

	c.Close()

	pkts := col.all()
	require.Len(t, pkts, 9)

	// packets are delivered with non-decreasing sort times and no duplicates
	requireMonotonicSortTime(t, pkts)

	type mediaSeq struct {
		mediaType description.MediaType
		seq       uint16
	}
	actual := make([]mediaSeq, len(pkts))
	for i, pkt := range pkts {
		actual[i] = mediaSeq{pkt.Media.Type, pkt.Packet.SequenceNumber}
		require.NotNil(t, pkt.Format)
	}
	// the two anchor packets are sorted with the PTS fallback and have
	// nearly identical sort times: check them as a set.
	require.ElementsMatch(t, []mediaSeq{
		{description.MediaTypeVideo, 1000},
		{description.MediaTypeAudio, 5000},
	}, actual[:2])
	require.Equal(t, []mediaSeq{
		{description.MediaTypeVideo, 1001},
		{description.MediaTypeAudio, 5001},
		{description.MediaTypeVideo, 1002},
		{description.MediaTypeAudio, 5002},
		{description.MediaTypeVideo, 1003},
		{description.MediaTypeVideo, 1004},
		{description.MediaTypeVideo, 1005},
	}, actual[2:])

	// packets after the RTCP anchor carry absolute NTP timestamps
	require.True(t, pkts[3].HasNTP)
	require.WithinDuration(t, t0.Add(1050*time.Millisecond), pkts[3].NTP, 10*time.Millisecond)
	require.WithinDuration(t, t0.Add(1050*time.Millisecond), pkts[3].SortTime, 10*time.Millisecond)
	require.True(t, pkts[7].HasNTP)
	require.WithinDuration(t, t0.Add(2500*time.Millisecond), pkts[7].SortTime, 10*time.Millisecond)

	// the original per-media callback kept working: 9 unique packets
	// (the duplicate has been removed)
	require.Equal(t, int64(9), callbackCount.Load())

	stats := c.SyncStats()
	require.Equal(t, uint64(9), stats.OutputPackets)
	require.Equal(t, uint64(0), stats.DroppedLate)
	require.Equal(t, uint64(0), stats.DroppedOverflow)
}

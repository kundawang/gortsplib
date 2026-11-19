package gortsplib

import (
	"bufio"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"

	"github.com/bluenviron/gortsplib/v5/internal/asyncprocessor"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/conn"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/headers"
	"github.com/bluenviron/gortsplib/v5/pkg/liberrors"
)

func TestServerSlowClientInvalidConfig(t *testing.T) {
	for _, ca := range []struct {
		name   string
		modify func(s *Server)
	}{
		{
			"negative policy",
			func(s *Server) {
				s.SlowClientPolicy = ServerSlowClientPolicy(-1)
			},
		},
		{
			"policy out of range",
			func(s *Server) {
				s.SlowClientPolicy = ServerSlowClientPolicy(3)
			},
		},
		{
			"negative max queue bytes",
			func(s *Server) {
				s.SlowClientMaxQueueBytes = -1
			},
		},
		{
			"negative max queue age",
			func(s *Server) {
				s.SlowClientMaxQueueAge = -time.Second
			},
		},
	} {
		t.Run(ca.name, func(t *testing.T) {
			s := &Server{
				RTSPAddress: "127.0.0.1:8654",
			}
			ca.modify(s)
			err := s.Start()
			require.Error(t, err)
		})
	}

	t.Run("valid", func(t *testing.T) {
		s := &Server{
			RTSPAddress:             "127.0.0.1:8654",
			SlowClientPolicy:        ServerSlowClientPolicyDropStale,
			SlowClientMaxQueueBytes: 1024,
			SlowClientMaxQueueAge:   time.Second,
		}
		err := s.Start()
		require.NoError(t, err)
		s.Close()
	})
}

func TestIsDroppablePacket(t *testing.T) {
	videoH264 := &description.Media{
		Type:    description.MediaTypeVideo,
		Formats: []format.Format{&format.H264{PayloadTyp: 96}},
	}
	videoH265 := &description.Media{
		Type:    description.MediaTypeVideo,
		Formats: []format.Format{&format.H265{PayloadTyp: 96}},
	}
	videoVP8 := &description.Media{
		Type:    description.MediaTypeVideo,
		Formats: []format.Format{&format.VP8{PayloadTyp: 96}},
	}
	audio := &description.Media{
		Type:    description.MediaTypeAudio,
		Formats: []format.Format{&format.G711{PayloadTyp: 0}},
	}

	for _, ca := range []struct {
		name      string
		medi      *description.Media
		payload   []byte
		droppable bool
	}{
		// audio packets are never droppable, therefore an audio-only
		// stream deterministically behaves like the default policy.
		{"audio", audio, []byte{0x01, 0x02}, false},
		// H264 IDR: keyframe, must be preserved.
		{"h264 idr", videoH264, []byte{0x65, 0x01}, false},
		// H264 SPS: parameter set, must be preserved.
		{"h264 sps", videoH264, []byte{0x67, 0x01}, false},
		// H264 PPS: parameter set, must be preserved.
		{"h264 pps", videoH264, []byte{0x68, 0x01}, false},
		// H264 non-IDR: droppable. This also covers packets that
		// arrive before the first keyframe of the stream.
		{"h264 non-idr", videoH264, []byte{0x41, 0x01}, true},
		// H264 STAP-A containing an IDR: must be preserved.
		{"h264 stap-a with idr", videoH264, []byte{0x78, 0x00, 0x02, 0x65, 0x01}, false},
		// H264 FU-A start of an IDR: must be preserved.
		{"h264 fu-a idr start", videoH264, []byte{0x7C, 0x85, 0x01}, false},
		// H264 FU-A start of a non-IDR: droppable.
		{"h264 fu-a non-idr start", videoH264, []byte{0x7C, 0x81, 0x01}, true},
		// H265 IDR: keyframe, must be preserved.
		{"h265 idr", videoH265, []byte{0x26, 0x01}, false},
		// H265 VPS: parameter set, must be preserved.
		{"h265 vps", videoH265, []byte{0x40, 0x01}, false},
		// H265 non-IDR: droppable.
		{"h265 non-idr", videoH265, []byte{0x02, 0x01}, true},
		// keyframes of codecs without a detector are never dropped.
		{"vp8", videoVP8, []byte{0x00, 0x01}, false},
	} {
		t.Run(ca.name, func(t *testing.T) {
			pkt := &rtp.Packet{Payload: ca.payload}
			require.Equal(t, ca.droppable,
				isDroppablePacket(ca.medi, ca.medi.Formats[0], pkt))
		})
	}
}

func newTestSlowClientSession(
	policy ServerSlowClientPolicy,
	maxBytes int,
	maxAge time.Duration,
	queueSize int,
) *ServerSession {
	ctx, ctxCancel := context.WithCancel(context.Background())

	s := &Server{
		SlowClientPolicy:        policy,
		SlowClientMaxQueueBytes: maxBytes,
		SlowClientMaxQueueAge:   maxAge,
		timeNow:                 time.Now,
	}

	ss := &ServerSession{
		s:         s,
		ctx:       ctx,
		ctxCancel: ctxCancel,
	}

	ss.writer = &asyncprocessor.Processor{
		BufferSize: queueSize,
		OnError:    func(context.Context, error) {},
	}
	ss.writer.Initialize()

	ss.backpressure = &serverSessionBackpressure{ss: ss}

	return ss
}

func testSlowClientEntry(size int, droppable bool, run func() error) *serverSessionQueueEntry {
	return &serverSessionQueueEntry{
		size:       size,
		enqueuedAt: time.Now(),
		droppable:  droppable,
		run:        run,
	}
}

func TestServerSlowClientBackpressureByteLimit(t *testing.T) {
	ss := newTestSlowClientSession(ServerSlowClientPolicyDropStale, 1000, 0, 4)
	defer ss.Close()
	defer ss.writer.Close()

	// first packet fits into the byte limit.
	err := ss.backpressure.push(testSlowClientEntry(800, true, func() error { return nil }))
	require.NoError(t, err)
	require.Equal(t, int64(800), ss.backpressure.queuedBytes.Load())

	// second packet exceeds the byte limit and is droppable: dropped.
	err = ss.backpressure.push(testSlowClientEntry(800, true, func() error { return nil }))
	require.NoError(t, err)
	require.Equal(t, uint64(1), ss.backpressure.droppedPackets.Load())
	require.Equal(t, uint64(800), ss.backpressure.droppedBytes.Load())
	require.Equal(t, int64(800), ss.backpressure.queuedBytes.Load())

	// third packet exceeds the byte limit but is not droppable
	// (keyframe, audio or RTCP): the limit is soft, packet is queued.
	err = ss.backpressure.push(testSlowClientEntry(800, false, func() error { return nil }))
	require.NoError(t, err)
	require.Equal(t, uint64(1), ss.backpressure.droppedPackets.Load())
	require.Equal(t, int64(1600), ss.backpressure.queuedBytes.Load())
}

func TestServerSlowClientBackpressureQueueFull(t *testing.T) {
	ss := newTestSlowClientSession(ServerSlowClientPolicyDropStale, 0, 0, 2)
	defer ss.Close()
	defer ss.writer.Close()

	// fill the queue.
	require.NoError(t, ss.backpressure.push(testSlowClientEntry(100, false, func() error { return nil })))
	require.NoError(t, ss.backpressure.push(testSlowClientEntry(100, false, func() error { return nil })))

	// a non-droppable packet (e.g. a keyframe of an all-keyframe stream,
	// or an audio packet) gets the default error.
	err := ss.backpressure.push(testSlowClientEntry(100, false, func() error { return nil }))
	require.Equal(t, liberrors.ErrServerWriteQueueFull{}, err)

	// a droppable packet is silently discarded.
	err = ss.backpressure.push(testSlowClientEntry(100, true, func() error { return nil }))
	require.NoError(t, err)
	require.Equal(t, uint64(1), ss.backpressure.droppedPackets.Load())
}

func TestServerSlowClientBackpressureDisconnect(t *testing.T) {
	t.Run("byte limit", func(t *testing.T) {
		ss := newTestSlowClientSession(ServerSlowClientPolicyDisconnect, 100, 0, 4)
		defer ss.writer.Close()

		err := ss.backpressure.push(testSlowClientEntry(200, true, func() error { return nil }))

		var serr liberrors.ErrServerSlowClient
		require.ErrorAs(t, err, &serr)
		require.NotEmpty(t, serr.Reason)

		<-ss.ctx.Done()
		require.Equal(t, serr, ss.closeErr)
	})

	t.Run("queue full", func(t *testing.T) {
		ss := newTestSlowClientSession(ServerSlowClientPolicyDisconnect, 0, 0, 2)
		defer ss.writer.Close()

		require.NoError(t, ss.backpressure.push(testSlowClientEntry(100, false, func() error { return nil })))
		require.NoError(t, ss.backpressure.push(testSlowClientEntry(100, false, func() error { return nil })))

		err := ss.backpressure.push(testSlowClientEntry(100, false, func() error { return nil }))

		var serr liberrors.ErrServerSlowClient
		require.ErrorAs(t, err, &serr)

		<-ss.ctx.Done()
		require.Equal(t, serr, ss.closeErr)
	})
}

func TestServerSlowClientBackpressureMaxAge(t *testing.T) {
	ss := newTestSlowClientSession(ServerSlowClientPolicyDropStale, 0, 100*time.Millisecond, 8)
	defer ss.Close()
	defer ss.writer.Close()

	delivered := make(chan struct{}, 8)
	ss.writer.Start()

	// a stale droppable packet is discarded at delivery time.
	entry := testSlowClientEntry(100, true, func() error {
		delivered <- struct{}{}
		return nil
	})
	entry.enqueuedAt = time.Now().Add(-time.Second)
	require.NoError(t, ss.backpressure.push(entry))

	require.Eventually(t, func() bool {
		return ss.backpressure.droppedPackets.Load() == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(0), ss.backpressure.queuedBytes.Load())
	require.Len(t, delivered, 0)

	// a fresh droppable packet is delivered.
	require.NoError(t, ss.backpressure.push(testSlowClientEntry(100, true, func() error {
		delivered <- struct{}{}
		return nil
	})))

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("packet not delivered")
	}

	// a stale non-droppable packet is delivered anyway.
	entry = testSlowClientEntry(100, false, func() error {
		delivered <- struct{}{}
		return nil
	})
	entry.enqueuedAt = time.Now().Add(-time.Second)
	require.NoError(t, ss.backpressure.push(entry))

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("packet not delivered")
	}
}

func TestServerSlowClientBackpressureConcurrentPush(t *testing.T) {
	ss := newTestSlowClientSession(ServerSlowClientPolicyDropStale, 0, 0, 256)
	defer ss.Close()
	defer ss.writer.Close()

	var delivered atomic.Int64
	ss.writer.Start()

	const goroutines = 8
	const perGoroutine = 200

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				// entries are all droppable: pushes never fail,
				// entries are either delivered or dropped.
				err := ss.backpressure.push(testSlowClientEntry(100, true, func() error {
					delivered.Add(1)
					return nil
				}))
				require.NoError(t, err)
			}
		}(i)
	}
	wg.Wait()

	total := int64(goroutines * perGoroutine)
	require.Eventually(t, func() bool {
		return delivered.Load()+int64(ss.backpressure.droppedPackets.Load()) == total //nolint:gosec
	}, 5*time.Second, 10*time.Millisecond)

	require.Equal(t, int64(0), ss.backpressure.queuedPackets.Load())
	require.Equal(t, int64(0), ss.backpressure.queuedBytes.Load())
}

// testWriteBufferListener shrinks the write buffer of accepted
// connections, in order to make slow clients deterministic in tests.
type testWriteBufferListener struct {
	net.Listener
	size int
}

func (l *testWriteBufferListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetWriteBuffer(l.size) //nolint:errcheck
	}
	return c, nil
}

func startSlowClientTestServer(
	t *testing.T,
	policy ServerSlowClientPolicy,
	udp bool,
	onSessionClose func(ctx *ServerHandlerOnSessionCloseCtx),
	sessions *[]*ServerSession,
	sessionsMu *sync.Mutex,
) (*Server, *ServerStream) {
	t.Helper()

	var stream *ServerStream

	s := &Server{
		RTSPAddress:             "127.0.0.1:8654",
		WriteQueueSize:          16,
		SlowClientPolicy:        policy,
		SlowClientMaxQueueBytes: 2048,
		Listen: func(network, address string) (net.Listener, error) {
			l, err := net.Listen(network, address)
			if err != nil {
				return nil, err
			}
			return &testWriteBufferListener{Listener: l, size: 2048}, nil
		},
		Handler: &testServerHandler{
			onSessionOpen: func(ctx *ServerHandlerOnSessionOpenCtx) {
				sessionsMu.Lock()
				*sessions = append(*sessions, ctx.Session)
				sessionsMu.Unlock()
			},
			onSessionClose: onSessionClose,
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

	if udp {
		s.UDPRTPAddress = "127.0.0.1:8100"
		s.UDPRTCPAddress = "127.0.0.1:8101"
	}

	stream = &ServerStream{
		Server: s,
		Desc:   &description.Session{Medias: []*description.Media{testH264Media}},
	}

	err := s.Start()
	require.NoError(t, err)

	err = stream.Initialize()
	require.NoError(t, err)

	return s, stream
}

// startSlowClientTestWriter writes a packet every 5ms; one packet
// out of 10 is a keyframe.
func startSlowClientTestWriter(
	stream *ServerStream,
	medi *description.Media,
	stop <-chan struct{},
) (written *atomic.Int64, keyframes *atomic.Int64) {
	written = &atomic.Int64{}
	keyframes = &atomic.Int64{}

	go func() {
		i := 0
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}

			payload := make([]byte, 1200)
			if i%10 == 0 {
				payload[0] = 0x65 // H264 IDR
				keyframes.Add(1)
			} else {
				payload[0] = 0x41 // H264 non-IDR
			}

			err := stream.WritePacketRTP(medi, &rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    96,
					SequenceNumber: uint16(i),        //nolint:gosec
					Timestamp:      uint32(i * 3000), //nolint:gosec
				},
				Payload: payload,
			})
			if err == nil {
				written.Add(1)
			}
			i++
		}
	}()

	return written, keyframes
}

func startSlowClientTestFastClient(t *testing.T, protocol Protocol, recv *atomic.Int64) *Client {
	t.Helper()

	u := mustParseURL("rtsp://127.0.0.1:8654/teststream")

	c := &Client{
		Scheme:   u.Scheme,
		Host:     u.Host,
		Protocol: &protocol,
	}

	err := c.Start()
	require.NoError(t, err)

	desc, _, err := c.Describe(u)
	require.NoError(t, err)

	err = c.SetupAll(desc.BaseURL, desc.Medias)
	require.NoError(t, err)

	c.OnPacketRTPAny(func(_ *description.Media, _ format.Format, _ *rtp.Packet) {
		recv.Add(1)
	})

	_, err = c.Play(nil)
	require.NoError(t, err)

	return c
}

func TestServerSlowClientDropStale(t *testing.T) {
	var sessionsMu sync.Mutex
	var sessions []*ServerSession

	slowClosed := make(chan error, 1)

	s, stream := startSlowClientTestServer(t, ServerSlowClientPolicyDropStale, false,
		func(ctx *ServerHandlerOnSessionCloseCtx) {
			sessionsMu.Lock()
			isSlow := len(sessions) > 0 && ctx.Session == sessions[0]
			sessionsMu.Unlock()
			if isSlow {
				slowClosed <- ctx.Error
			}
		}, &sessions, &sessionsMu)
	defer s.Close()
	defer stream.Close()

	// slow client: raw connection with a small receive buffer,
	// reads one frame every 20ms.

	nconn, err := net.Dial("tcp", "127.0.0.1:8654")
	require.NoError(t, err)
	defer nconn.Close()

	err = nconn.(*net.TCPConn).SetReadBuffer(2048)
	require.NoError(t, err)

	bconn := conn.NewConn(bufio.NewReader(nconn), nconn)

	desc := doDescribe(t, bconn, false)

	res, _ := doSetup(t, bconn, mediaURL(t, desc.BaseURL, desc.Medias[0]).String(), &headers.Transport{
		Protocol:       headers.TransportProtocolTCP,
		Delivery:       new(headers.TransportDeliveryUnicast),
		InterleavedIDs: &[2]int{0, 1},
		Mode:           new(headers.TransportModePlay),
	}, "")

	doPlay(t, bconn, "rtsp://127.0.0.1:8654/teststream", readSession(t, res))

	var slowRecv, slowKeyframes atomic.Int64
	readerStop := make(chan struct{})
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)
		for {
			select {
			case <-readerStop:
				return
			default:
			}

			what, err2 := bconn.Read()
			if err2 != nil {
				return
			}

			if frame, ok := what.(*base.InterleavedFrame); ok && frame.Channel == 0 {
				var pkt rtp.Packet
				if err3 := pkt.Unmarshal(frame.Payload); err3 == nil &&
					len(pkt.Payload) > 0 && pkt.Payload[0]&0x1F == 5 {
					slowKeyframes.Add(1)
				}
				slowRecv.Add(1)
			}

			time.Sleep(20 * time.Millisecond)
		}
	}()

	// fast client.

	var fastRecv atomic.Int64
	c := startSlowClientTestFastClient(t, ProtocolTCP, &fastRecv)
	defer c.Close()

	stop := make(chan struct{})
	written, keyframes := startSlowClientTestWriter(stream, stream.Desc.Medias[0], stop)

	time.Sleep(1500 * time.Millisecond)
	close(stop)

	// the fast client receives everything, independently from the slow one.
	require.Eventually(t, func() bool {
		return fastRecv.Load() == written.Load()
	}, 2*time.Second, 10*time.Millisecond)

	// the slow client drops packets but receives all keyframes.
	time.Sleep(500 * time.Millisecond)
	t.Logf("slowKeyframes=%d keyframes=%d slowRecv=%d written=%d dropped=%d",
		slowKeyframes.Load(), keyframes.Load(), slowRecv.Load(), written.Load(),
		sessions[0].Stats().OutboundRTPPacketsDropped)
	require.Eventually(t, func() bool {
		return slowKeyframes.Load() == keyframes.Load()
	}, 2*time.Second, 10*time.Millisecond)

	sessionsMu.Lock()
	slowSession := sessions[0]
	sessionsMu.Unlock()

	stats := slowSession.Stats()
	require.Greater(t, stats.OutboundRTPPacketsDropped, uint64(0))
	require.Less(t, slowRecv.Load(), written.Load())

	// a client-initiated close must not be reported as a slow client.
	close(readerStop)
	<-readerDone
	doTeardown(t, bconn, "rtsp://127.0.0.1:8654/teststream", readSession(t, res))

	select {
	case err2 := <-slowClosed:
		var terr liberrors.ErrServerSessionTornDown
		require.ErrorAs(t, err2, &terr)
	case <-time.After(2 * time.Second):
		t.Fatal("slow session not closed")
	}

	// resources of the slow session are released; the fast one remains.
	require.Eventually(t, func() bool {
		stream.mutex.RLock()
		defer stream.mutex.RUnlock()
		return len(stream.readers) == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestServerSlowClientDisconnect(t *testing.T) {
	var sessionsMu sync.Mutex
	var sessions []*ServerSession

	slowClosed := make(chan error, 1)

	s, stream := startSlowClientTestServer(t, ServerSlowClientPolicyDisconnect, false,
		func(ctx *ServerHandlerOnSessionCloseCtx) {
			sessionsMu.Lock()
			isSlow := len(sessions) > 0 && ctx.Session == sessions[0]
			sessionsMu.Unlock()
			if isSlow {
				slowClosed <- ctx.Error
			}
		}, &sessions, &sessionsMu)
	defer s.Close()
	defer stream.Close()

	// slow client: never reads.

	nconn, err := net.Dial("tcp", "127.0.0.1:8654")
	require.NoError(t, err)
	defer nconn.Close()

	bconn := conn.NewConn(bufio.NewReader(nconn), nconn)

	desc := doDescribe(t, bconn, false)

	res, _ := doSetup(t, bconn, mediaURL(t, desc.BaseURL, desc.Medias[0]).String(), &headers.Transport{
		Protocol:       headers.TransportProtocolTCP,
		Delivery:       new(headers.TransportDeliveryUnicast),
		InterleavedIDs: &[2]int{0, 1},
		Mode:           new(headers.TransportModePlay),
	}, "")

	doPlay(t, bconn, "rtsp://127.0.0.1:8654/teststream", readSession(t, res))

	// fast client.

	var fastRecv atomic.Int64
	c := startSlowClientTestFastClient(t, ProtocolTCP, &fastRecv)
	defer c.Close()

	stop := make(chan struct{})
	defer close(stop)
	written, _ := startSlowClientTestWriter(stream, stream.Desc.Medias[0], stop)

	// the slow session is disconnected with a dedicated error.
	select {
	case err2 := <-slowClosed:
		var serr liberrors.ErrServerSlowClient
		require.ErrorAs(t, err2, &serr)
		require.NotEmpty(t, serr.Reason)
	case <-time.After(5 * time.Second):
		t.Fatal("slow session not disconnected")
	}

	// resources of the slow session are released.
	require.Eventually(t, func() bool {
		stream.mutex.RLock()
		defer stream.mutex.RUnlock()
		return len(stream.readers) == 1
	}, 2*time.Second, 10*time.Millisecond)

	// the fast client is unaffected.
	require.Eventually(t, func() bool {
		return fastRecv.Load() == written.Load()
	}, 2*time.Second, 10*time.Millisecond)
}

func TestServerSlowClientUDP(t *testing.T) {
	var sessionsMu sync.Mutex
	var sessions []*ServerSession

	s, stream := startSlowClientTestServer(t, ServerSlowClientPolicyDropStale, true,
		func(_ *ServerHandlerOnSessionCloseCtx) {}, &sessions, &sessionsMu)
	defer s.Close()
	defer stream.Close()

	// slow client: UDP transport, never reads incoming packets.
	// UDP writes are non-blocking, therefore the server-side queue
	// does not accumulate and no packet is dropped server-side:
	// packet loss happens on the client side, inside OS buffers.

	nconn, err := net.Dial("tcp", "127.0.0.1:8654")
	require.NoError(t, err)
	defer nconn.Close()

	bconn := conn.NewConn(bufio.NewReader(nconn), nconn)

	desc := doDescribe(t, bconn, false)

	res, _ := doSetup(t, bconn, mediaURL(t, desc.BaseURL, desc.Medias[0]).String(), &headers.Transport{
		Protocol:    headers.TransportProtocolUDP,
		Delivery:    new(headers.TransportDeliveryUnicast),
		ClientPorts: &[2]int{35466, 35467},
		Mode:        new(headers.TransportModePlay),
	}, "")

	doPlay(t, bconn, "rtsp://127.0.0.1:8654/teststream", readSession(t, res))

	// fast client.

	var fastRecv atomic.Int64
	c := startSlowClientTestFastClient(t, ProtocolUDP, &fastRecv)
	defer c.Close()

	stop := make(chan struct{})
	written, _ := startSlowClientTestWriter(stream, stream.Desc.Medias[0], stop)

	time.Sleep(time.Second)
	close(stop)

	require.Eventually(t, func() bool {
		return fastRecv.Load() == written.Load()
	}, 2*time.Second, 10*time.Millisecond)

	sessionsMu.Lock()
	slowSession := sessions[0]
	sessionsMu.Unlock()

	require.Eventually(t, func() bool {
		stats := slowSession.Stats()
		return stats.OutboundRTPPackets == uint64(written.Load())
	}, 2*time.Second, 10*time.Millisecond)

	stats := slowSession.Stats()
	require.Equal(t, uint64(0), stats.OutboundRTPPacketsDropped)

	doTeardown(t, bconn, "rtsp://127.0.0.1:8654/teststream", readSession(t, res))
}

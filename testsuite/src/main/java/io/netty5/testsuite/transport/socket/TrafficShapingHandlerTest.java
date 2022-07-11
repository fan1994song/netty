/*
 * Copyright 2012 The Netty Project
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 * https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty5.testsuite.transport.socket;

import io.netty5.bootstrap.Bootstrap;
import io.netty5.bootstrap.ServerBootstrap;
import io.netty5.buffer.api.Buffer;
import io.netty5.channel.Channel;
import io.netty5.channel.ChannelHandlerContext;
import io.netty5.channel.ChannelInitializer;
import io.netty5.channel.ChannelOption;
import io.netty5.channel.SimpleChannelInboundHandler;
import io.netty5.channel.socket.SocketChannel;
import io.netty5.handler.traffic.AbstractTrafficShapingHandler;
import io.netty5.handler.traffic.ChannelTrafficShapingHandler;
import io.netty5.handler.traffic.GlobalTrafficShapingHandler;
import io.netty5.handler.traffic.TrafficCounter;
import io.netty5.util.concurrent.DefaultEventExecutorGroup;
import io.netty5.util.concurrent.EventExecutorGroup;
import io.netty5.util.concurrent.Promise;
import io.netty5.util.internal.logging.InternalLogger;
import io.netty5.util.internal.logging.InternalLoggerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TrafficShapingHandlerTest extends AbstractSocketTest {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(TrafficShapingHandlerTest.class);
    private static final InternalLogger loggerServer = InternalLoggerFactory.getInstance("ServerTSH");
    private static final InternalLogger loggerClient = InternalLoggerFactory.getInstance("ClientTSH");

    static final int messageSize = 1024;
    static final int bandwidthFactor = 12;
    static final int minfactor = 3;
    static final int maxfactor = bandwidthFactor + bandwidthFactor / 2;
    static final long stepms = (1000 / bandwidthFactor - 10) / 10 * 10;
    static final long minimalms = Math.max(stepms / 2, 20) / 10 * 10;
    static final long check = 10;
    private static final Random random = new Random();
    static final byte[] data = new byte[messageSize];

    private static final String TRAFFIC = "traffic";
    private static String currentTestName;
    private static int currentTestRun;

    private static EventExecutorGroup group;
    private static EventExecutorGroup groupForGlobal;
    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    static {
        random.nextBytes(data);
    }

    @BeforeAll
    public static void createGroup() {
        logger.info("Bandwidth: " + minfactor + " <= " + bandwidthFactor + " <= " + maxfactor +
                    " StepMs: " + stepms + " MinMs: " + minimalms + " CheckMs: " + check);
        group = new DefaultEventExecutorGroup(8);
        groupForGlobal = new DefaultEventExecutorGroup(8);
    }

    @AfterAll
    public static void destroyGroup() throws Exception {
        group.shutdownGracefully().asStage().sync();
        groupForGlobal.shutdownGracefully().asStage().sync();
        executor.shutdown();
    }

    private static long[] computeWaitRead(int[] multipleMessage) {
        long[] minimalWaitBetween = new long[multipleMessage.length + 1];
        minimalWaitBetween[0] = 0;
        for (int i = 0; i < multipleMessage.length; i++) {
            if (multipleMessage[i] > 1) {
                minimalWaitBetween[i + 1] = (multipleMessage[i] - 1) * stepms + minimalms;
            } else {
                minimalWaitBetween[i + 1] = 10;
            }
        }
        return minimalWaitBetween;
    }

    private static long[] computeWaitWrite(int[] multipleMessage) {
        long[] minimalWaitBetween = new long[multipleMessage.length + 1];
        for (int i = 0; i < multipleMessage.length; i++) {
            if (multipleMessage[i] > 1) {
                minimalWaitBetween[i] = (multipleMessage[i] - 1) * stepms + minimalms;
            } else {
                minimalWaitBetween[i] = 10;
            }
        }
        return minimalWaitBetween;
    }

    private static long[] computeWaitAutoRead(int []autoRead) {
        long [] minimalWaitBetween = new long[autoRead.length + 1];
        minimalWaitBetween[0] = 0;
        for (int i = 0; i < autoRead.length; i++) {
            if (autoRead[i] != 0) {
                if (autoRead[i] > 0) {
                    minimalWaitBetween[i + 1] = -1;
                } else {
                    minimalWaitBetween[i + 1] = check;
                }
            } else {
                minimalWaitBetween[i + 1] = 0;
            }
        }
        return minimalWaitBetween;
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testNoTrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST NO TRAFFIC";
        currentTestRun = 0;
        run(testInfo, this::testNoTrafficShaping);
    }

    public void testNoTrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = null;
        int[] multipleMessage = { 1, 2, 1 };
        long[] minimalWaitBetween = null;
        testTrafficShaping0(sb, cb, false, false, false, false, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testWriteTrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST WRITE";
        currentTestRun = 0;
        run(testInfo, this::testWriteTrafficShaping);
    }

    public void testWriteTrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = null;
        int[] multipleMessage = { 1, 2, 1, 1 };
        long[] minimalWaitBetween = computeWaitWrite(multipleMessage);
        testTrafficShaping0(sb, cb, false, false, true, false, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testReadTrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST READ";
        currentTestRun = 0;
        run(testInfo, this::testReadTrafficShaping);
    }

    public void testReadTrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = null;
        int[] multipleMessage = { 1, 2, 1, 1 };
        long[] minimalWaitBetween = computeWaitRead(multipleMessage);
        testTrafficShaping0(sb, cb, false, true, false, false, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testWrite1TrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST WRITE";
        currentTestRun = 0;
        run(testInfo, this::testWrite1TrafficShaping);
    }

    public void testWrite1TrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = null;
        int[] multipleMessage = { 1, 1, 1 };
        long[] minimalWaitBetween = computeWaitWrite(multipleMessage);
        testTrafficShaping0(sb, cb, false, false, true, false, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testRead1TrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST READ";
        currentTestRun = 0;
        run(testInfo, this::testRead1TrafficShaping);
    }

    public void testRead1TrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = null;
        int[] multipleMessage = { 1, 1, 1 };
        long[] minimalWaitBetween = computeWaitRead(multipleMessage);
        testTrafficShaping0(sb, cb, false, true, false, false, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testWriteGlobalTrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST GLOBAL WRITE";
        currentTestRun = 0;
        run(testInfo, this::testWriteGlobalTrafficShaping);
    }

    public void testWriteGlobalTrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = null;
        int[] multipleMessage = { 1, 2, 1, 1 };
        long[] minimalWaitBetween = computeWaitWrite(multipleMessage);
        testTrafficShaping0(sb, cb, false, false, true, true, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testReadGlobalTrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST GLOBAL READ";
        currentTestRun = 0;
        run(testInfo, this::testReadGlobalTrafficShaping);
    }

    public void testReadGlobalTrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = null;
        int[] multipleMessage = { 1, 2, 1, 1 };
        long[] minimalWaitBetween = computeWaitRead(multipleMessage);
        testTrafficShaping0(sb, cb, false, true, false, true, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testAutoReadTrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST AUTO READ";
        currentTestRun = 0;
        run(testInfo, this::testAutoReadTrafficShaping);
    }

    public void testAutoReadTrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = { 1, -1, -1, 1, -2, 0, 1, 0, -3, 0, 1, 2, 0 };
        int[] multipleMessage = new int[autoRead.length];
        Arrays.fill(multipleMessage, 1);
        long[] minimalWaitBetween = computeWaitAutoRead(autoRead);
        testTrafficShaping0(sb, cb, false, true, false, false, autoRead, minimalWaitBetween, multipleMessage);
    }

    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    public void testAutoReadGlobalTrafficShaping(TestInfo testInfo) throws Throwable {
        currentTestName = "TEST AUTO READ GLOBAL";
        currentTestRun = 0;
        run(testInfo, this::testAutoReadGlobalTrafficShaping);
    }

    public void testAutoReadGlobalTrafficShaping(ServerBootstrap sb, Bootstrap cb) throws Throwable {
        int[] autoRead = { 1, -1, -1, 1, -2, 0, 1, 0, -3, 0, 1, 2, 0 };
        int[] multipleMessage = new int[autoRead.length];
        Arrays.fill(multipleMessage, 1);
        long[] minimalWaitBetween = computeWaitAutoRead(autoRead);
        testTrafficShaping0(sb, cb, false, true, false, true, autoRead, minimalWaitBetween, multipleMessage);
    }

    /**
     *
     * @param additionalExecutor
     *            shall the pipeline add the handler using an additional executor
     * @param limitRead
     *            True to set Read Limit on Server side
     * @param limitWrite
     *            True to set Write Limit on Client side
     * @param globalLimit
     *            True to change Channel to Global TrafficShapping
     * @param minimalWaitBetween
     *            time in ms that should be waited before getting the final result (note: for READ the values are
     *            right shifted once, the first value being 0)
     * @param multipleMessage
     *            how many message to send at each step (for READ: the first should be 1, as the two last steps to
     *            ensure correct testing)
     * @throws Throwable if something goes wrong, and the test fails.
     */
    private static void testTrafficShaping0(
            ServerBootstrap sb, Bootstrap cb, final boolean additionalExecutor,
            final boolean limitRead, final boolean limitWrite, final boolean globalLimit, int[] autoRead,
            long[] minimalWaitBetween, int[] multipleMessage) throws Throwable {

        currentTestRun++;
        logger.info("TEST: " + currentTestName + " RUN: " + currentTestRun +
                    " Exec: " + additionalExecutor + " Read: " + limitRead + " Write: " + limitWrite + " Global: "
                    + globalLimit);
        final ServerHandler sh = new ServerHandler(autoRead, multipleMessage);
        Promise<Boolean> promise = group.next().newPromise();
        final ClientHandler ch = new ClientHandler(promise, minimalWaitBetween, multipleMessage,
                                                   autoRead);

        final AbstractTrafficShapingHandler handler;
        if (limitRead) {
            if (globalLimit) {
                handler = new GlobalTrafficShapingHandler(groupForGlobal, 0, bandwidthFactor * messageSize, check);
            } else {
                handler = new ChannelTrafficShapingHandler(0, bandwidthFactor * messageSize, check);
            }
        } else if (limitWrite) {
            if (globalLimit) {
                handler = new GlobalTrafficShapingHandler(groupForGlobal, bandwidthFactor * messageSize, 0, check);
            } else {
                handler = new ChannelTrafficShapingHandler(bandwidthFactor * messageSize, 0, check);
            }
        } else {
            handler = null;
        }

        sb.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel c) throws Exception {
                if (limitRead) {
                    c.pipeline().addLast(TRAFFIC, handler);
                }
                c.pipeline().addLast(sh);
            }
        });
        cb.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel c) throws Exception {
                if (limitWrite) {
                    c.pipeline().addLast(TRAFFIC, handler);
                }
                c.pipeline().addLast(ch);
            }
        });

        Channel sc = sb.bind().asStage().get();
        Channel cc = cb.connect(sc.localAddress()).asStage().get();

        int totalNb = 0;
        for (int i = 1; i < multipleMessage.length; i++) {
            totalNb += multipleMessage[i];
        }
        Long start = TrafficCounter.milliSecondFromNano();
        int nb = multipleMessage[0];
        for (int i = 0; i < nb; i++) {
            cc.write(cc.bufferAllocator().copyOf(data));
        }
        cc.flush();

        promise.asFuture().asStage().await();
        Long stop = TrafficCounter.milliSecondFromNano();
        assertTrue(promise.isSuccess(), "Error during execution of TrafficShapping: " + promise.cause());

        float average = (totalNb * messageSize) / (float) (stop - start);
        logger.info("TEST: " + currentTestName + " RUN: " + currentTestRun +
                    " Average of traffic: " + average + " compare to " + bandwidthFactor);
        sh.channel.close().asStage().sync();
        ch.channel.close().asStage().sync();
        sc.close().asStage().sync();
        if (autoRead != null) {
            // for extra release call in AutoRead
            Thread.sleep(minimalms);
        }

        if (autoRead == null && minimalWaitBetween != null) {
            assertTrue(average <= maxfactor,
                "Overall Traffic not ok since > " + maxfactor + ": " + average);
            if (additionalExecutor) {
                // Oio is not as good when using additionalExecutor
                assertTrue(average >= 0.25, "Overall Traffic not ok since < 0.25: " + average);
            } else {
                assertTrue(average >= minfactor,
                    "Overall Traffic not ok since < " + minfactor + ": " + average);
            }
        }
        if (handler != null && globalLimit) {
            ((GlobalTrafficShapingHandler) handler).release();
        }

        if (sh.exception.get() != null && !(sh.exception.get() instanceof IOException)) {
            throw sh.exception.get();
        }
        if (ch.exception.get() != null && !(ch.exception.get() instanceof IOException)) {
            throw ch.exception.get();
        }
        if (sh.exception.get() != null) {
            throw sh.exception.get();
        }
        if (ch.exception.get() != null) {
            throw ch.exception.get();
        }
    }

    private static class ClientHandler extends SimpleChannelInboundHandler<Buffer> {
        volatile Channel channel;
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        volatile int step;
        // first message will always be validated
        private long currentLastTime = TrafficCounter.milliSecondFromNano();
        private final long[] minimalWaitBetween;
        private final int[] multipleMessage;
        private final int[] autoRead;
        final Promise<Boolean> promise;

        ClientHandler(Promise<Boolean> promise, long[] minimalWaitBetween, int[] multipleMessage,
                      int[] autoRead) {
            this.minimalWaitBetween = minimalWaitBetween;
            this.multipleMessage = Arrays.copyOf(multipleMessage, multipleMessage.length);
            this.promise = promise;
            this.autoRead = autoRead;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channel = ctx.channel();
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, Buffer in) throws Exception {
            long lastTimestamp = 0;
            loggerClient.debug("Step: " + step + " Read: " + in.readableBytes() / 8 + " blocks");
            while (in.readableBytes() > 0) {
                lastTimestamp = in.readLong();
                multipleMessage[step]--;
            }
            if (multipleMessage[step] > 0) {
                // still some message to get
                return;
            }
            long minimalWait = minimalWaitBetween != null? minimalWaitBetween[step] : 0;
            int ar = 0;
            if (autoRead != null) {
                if (step > 0 && autoRead[step - 1] != 0) {
                    ar = autoRead[step - 1];
                }
            }
            loggerClient.info("Step: " + step + " Interval: " + (lastTimestamp - currentLastTime) + " compareTo "
                              + minimalWait + " (" + ar + ')');
            assertTrue(lastTimestamp - currentLastTime >= minimalWait,
                    "The interval of time is incorrect:" + (lastTimestamp - currentLastTime) + " not> " + minimalWait);
            currentLastTime = lastTimestamp;
            step++;
            if (multipleMessage.length > step) {
                int nb = multipleMessage[step];
                for (int i = 0; i < nb; i++) {
                    channel.write(channel.bufferAllocator().copyOf(data));
                }
                channel.flush();
            } else {
                promise.setSuccess(true);
            }
        }

        @Override
        public void channelExceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (exception.compareAndSet(null, cause)) {
                cause.printStackTrace();
                promise.setFailure(cause);
                ctx.close();
            }
        }
    }

    private static class ServerHandler extends SimpleChannelInboundHandler<Buffer> {
        private final int[] autoRead;
        private final int[] multipleMessage;
        volatile Channel channel;
        volatile int step;
        final AtomicReference<Throwable> exception = new AtomicReference<>();

        ServerHandler(int[] autoRead, int[] multipleMessage) {
            this.autoRead = autoRead;
            this.multipleMessage = Arrays.copyOf(multipleMessage, multipleMessage.length);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            channel = ctx.channel();
        }

        @Override
        public void messageReceived(final ChannelHandlerContext ctx, Buffer in) throws Exception {
            byte[] actual = new byte[in.readableBytes()];
            int nb = actual.length / messageSize;
            loggerServer.info("Step: " + step + " Read: " + nb + " blocks");
            in.readBytes(actual, 0, actual.length);
            long timestamp = TrafficCounter.milliSecondFromNano();
            int isAutoRead = 0;
            int laststep = step;
            for (int i = 0; i < nb; i++) {
                multipleMessage[step]--;
                if (multipleMessage[step] == 0) {
                    // setAutoRead test
                    if (autoRead != null) {
                        isAutoRead = autoRead[step];
                    }
                    step++;
                }
            }
            if (laststep != step) {
                // setAutoRead test
                if (autoRead != null && isAutoRead != 2) {
                    if (isAutoRead != 0) {
                        loggerServer.info("Step: " + step + " Set AutoRead: " + (isAutoRead > 0));
                        channel.setOption(ChannelOption.AUTO_READ, isAutoRead > 0);
                    } else {
                        loggerServer.info("Step: " + step + " AutoRead: NO");
                    }
                }
            }
            Thread.sleep(10);
            loggerServer.debug("Step: " + step + " Write: " + nb);
            for (int i = 0; i < nb; i++) {
                channel.write(ctx.bufferAllocator().allocate(8).writeLong(timestamp));
            }
            channel.flush();
            if (laststep != step) {
                // setAutoRead test
                if (isAutoRead != 0) {
                    if (isAutoRead < 0) {
                        final int exactStep = step;
                        long wait = isAutoRead == -1? minimalms : stepms + minimalms;
                        if (isAutoRead == -3) {
                            wait = stepms * 3;
                        }
                        executor.schedule(() -> {
                            loggerServer.info("Step: " + exactStep + " Reset AutoRead");
                            channel.setOption(ChannelOption.AUTO_READ, true);
                        }, wait, TimeUnit.MILLISECONDS);
                    } else {
                        if (isAutoRead > 1) {
                            loggerServer.debug("Step: " + step + " Will Set AutoRead: True");
                            final int exactStep = step;
                            executor.schedule(() -> {
                                loggerServer.info("Step: " + exactStep + " Set AutoRead: True");
                                channel.setOption(ChannelOption.AUTO_READ, true);
                            }, stepms + minimalms, TimeUnit.MILLISECONDS);
                        }
                    }
                }
            }
        }

        @Override
        public void channelExceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (exception.compareAndSet(null, cause)) {
                cause.printStackTrace();
                ctx.close();
            }
        }
    }
}
